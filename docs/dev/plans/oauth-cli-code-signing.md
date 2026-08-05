# Research: Code Signing for foundry-dev-tools-oauth CLI

## Problem

macOS Keychain attaches an Access Control List (ACL) to each stored credential. For unsigned binaries, the ACL records the binary's path and hash. Every recompilation changes the hash, causing macOS to prompt "foundry-dev-tools-oauth wants to access your keychain" on first access after every update.

Windows Credential Manager does **not** gate access by code signing identity -- it uses the logged-in user session. Code signing on Windows is only about SmartScreen reputation and user trust.

## How macOS Keychain Identifies Applications

Keychain does not match by file path or binary hash. It stores a **Designated Requirement (DR)** extracted from the binary's code signature. For a Developer ID-signed binary, the DR looks like:

```
identifier "com.yourcompany.foundry-dev-tools-oauth-cli"
  and anchor apple generic
  and certificate leaf[subject.OU] = "K1234ABCDE"
```

This checks:
1. The **code signing identifier** (the `--identifier` / `-i` value passed to `codesign`)
2. The certificate chain is rooted in Apple's CA
3. The leaf certificate is a Developer ID Application certificate
4. The **Team ID** (`subject.OU`) matches

The DR does **not** depend on the binary hash, file path, file size, modification date, or specific certificate serial number. This means **any future build signed with the same identifier and Team ID is granted Keychain access silently**.

### What would break Keychain access

- Changing the code signing identifier
- Switching to a different Apple Developer account (different Team ID)
- Distributing an unsigned binary
- Switching certificate types (e.g., Developer ID to ad-hoc)

## Requirements

### macOS

| Item | Notes |
|------|-------|
| Apple Developer Program | $99/year, mandatory, no free path |
| Developer ID Application certificate | Created in Apple Developer portal, valid 5 years |
| Stable code signing identifier | Must never change once users store Keychain items |
| Notarization | Required for Gatekeeper since macOS 10.15 |

### Windows (optional)

| Item | Notes |
|------|-------|
| OV code signing certificate | $200-500/year |
| Hardware-backed private key | Required since June 2023 (FIPS 140-2 Level 2) |
| Cloud HSM (Azure Key Vault or SSL.com eSigner) | Physical USB tokens impractical for CI |

## Chosen Code Signing Identifier

**This value is permanent. Changing it breaks Keychain access for all existing users.**

```
com.palantir.foundry-dev-tools-oauth-cli
```

## Certificate Setup (One-Time)

### macOS

1. Enroll in the Apple Developer Program at https://developer.apple.com ($99/year)
2. Go to Certificates, Identifiers & Profiles
3. Create a **Developer ID Application** certificate
4. Generate a CSR via Keychain Access on a Mac
5. Upload CSR, download the `.cer`, import into Keychain Access
6. Export as `.p12` (select cert + private key, right-click, "Export 2 items")
7. Base64-encode for GitHub Secrets:
   ```bash
   base64 -i DeveloperIDApplication.p12 -o cert_base64.txt
   ```
8. Generate an app-specific password at https://appleid.apple.com (for notarization)

### Windows (if needed)

1. Create an Azure Key Vault instance (Premium tier, ~$1/month)
2. Generate a certificate request with RSA-HSM key type
3. Submit CSR to a CA (DigiCert, Sectigo, SSL.com) for an OV code signing certificate
4. Merge the issued certificate back into Key Vault
5. Create an Azure AD service principal with Sign + Get permissions on the vault

## GitHub Secrets

### macOS (required)

| Secret | Value |
|--------|-------|
| `MACOS_CERTIFICATE` | Base64-encoded `.p12` file |
| `MACOS_CERTIFICATE_PWD` | Password used when exporting `.p12` |
| `MACOS_CERTIFICATE_NAME` | `"Developer ID Application: Name (TEAMID)"` |
| `APPLE_ID` | Apple ID email address |
| `APPLE_TEAM_ID` | 10-character Team ID from developer.apple.com |
| `APPLE_APP_PASSWORD` | App-specific password from appleid.apple.com |
| `KEYCHAIN_PWD` | Any random password (for temporary CI keychain) |

### Windows (optional)

| Secret | Value |
|--------|-------|
| `AZURE_KEY_VAULT_URI` | `https://your-vault.vault.azure.net` |
| `AZURE_CLIENT_ID` | Service principal client ID |
| `AZURE_TENANT_ID` | Azure AD tenant ID |
| `AZURE_CLIENT_SECRET` | Service principal secret |
| `AZURE_CERT_NAME` | Certificate name in Key Vault |

## GitHub Actions Workflow

### macOS

```yaml
jobs:
  build-macos:
    runs-on: macos-latest
    strategy:
      matrix:
        target: [x86_64-apple-darwin, aarch64-apple-darwin]
    steps:
      - uses: actions/checkout@v4

      - name: Install Rust toolchain
        run: rustup target add ${{ matrix.target }}

      - name: Import code signing certificate
        uses: apple-actions/import-codesign-certs@v3
        with:
          p12-file-base64: ${{ secrets.MACOS_CERTIFICATE }}
          p12-password: ${{ secrets.MACOS_CERTIFICATE_PWD }}

      - name: Build
        run: cargo build --release --target ${{ matrix.target }}
        working-directory: libs/oauth-cli

      - name: Sign binary
        run: |
          codesign --sign "${{ secrets.MACOS_CERTIFICATE_NAME }}" \
            --force \
            --timestamp \
            --options runtime \
            --identifier "com.palantir.foundry-dev-tools-oauth-cli" \
            target/${{ matrix.target }}/release/foundry-dev-tools-oauth
        working-directory: libs/oauth-cli

      - name: Verify signature
        run: |
          codesign --verify --verbose=2 \
            target/${{ matrix.target }}/release/foundry-dev-tools-oauth
          codesign -d -r- \
            target/${{ matrix.target }}/release/foundry-dev-tools-oauth
        working-directory: libs/oauth-cli

      - name: Notarize binary
        run: |
          xcrun notarytool store-credentials "notary-profile" \
            --apple-id "${{ secrets.APPLE_ID }}" \
            --team-id "${{ secrets.APPLE_TEAM_ID }}" \
            --password "${{ secrets.APPLE_APP_PASSWORD }}"

          cd libs/oauth-cli
          zip -j foundry-dev-tools-oauth-${{ matrix.target }}.zip \
            target/${{ matrix.target }}/release/foundry-dev-tools-oauth

          xcrun notarytool submit \
            foundry-dev-tools-oauth-${{ matrix.target }}.zip \
            --keychain-profile "notary-profile" \
            --wait

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: foundry-dev-tools-oauth-${{ matrix.target }}
          path: libs/oauth-cli/foundry-dev-tools-oauth-${{ matrix.target }}.zip
```

### Windows (optional)

```yaml
jobs:
  build-windows:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v4

      - name: Build
        run: cargo build --release
        working-directory: libs/oauth-cli

      - name: Sign binary with AzureSignTool
        run: |
          dotnet tool install --global AzureSignTool
          AzureSignTool sign `
            -kvu "${{ secrets.AZURE_KEY_VAULT_URI }}" `
            -kvi "${{ secrets.AZURE_CLIENT_ID }}" `
            -kvt "${{ secrets.AZURE_TENANT_ID }}" `
            -kvs "${{ secrets.AZURE_CLIENT_SECRET }}" `
            -kvc "${{ secrets.AZURE_CERT_NAME }}" `
            -tr http://timestamp.digicert.com `
            -td sha256 `
            libs/oauth-cli/target/release/foundry-dev-tools-oauth.exe
```

### Linux (no signing needed)

```yaml
jobs:
  build-linux:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        target: [x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu]
    steps:
      - uses: actions/checkout@v4

      - name: Build
        run: cargo build --release --target ${{ matrix.target }}
        working-directory: libs/oauth-cli

      - name: Upload artifact
        uses: actions/upload-artifact@v4
        with:
          name: foundry-dev-tools-oauth-${{ matrix.target }}
          path: libs/oauth-cli/target/${{ matrix.target }}/release/foundry-dev-tools-oauth
```

## codesign Flags Reference

| Flag | Purpose |
|------|---------|
| `--sign` / `-s` | Signing identity (full cert CN or Team ID hash) |
| `--force` / `-f` | Replace any existing signature |
| `--timestamp` | Embed secure timestamp (required for notarization, ensures signature validity after cert expiry) |
| `--options runtime` | Enable Hardened Runtime (required for notarization) |
| `--identifier` / `-i` | Code signing identifier in reverse-DNS format |

## Notarization Notes

- `xcrun notarytool submit` requires a `.zip`, `.pkg`, or `.dmg` -- not a bare Mach-O binary
- Processing typically takes 1-5 minutes, can take 30+ minutes
- `--wait` flag polls until complete
- **Stapling is not possible for bare Mach-O binaries** -- `xcrun stapler` only works on `.app`, `.pkg`, `.dmg`
- First-run Gatekeeper verification requires an internet connection (since stapling is not possible)
- To enable offline verification, distribute inside a `.pkg` and staple the ticket to the `.pkg`
- Check failure details: `xcrun notarytool log <submission-id> --keychain-profile "notary-profile"`

## Certificate Renewal

- Developer ID Application certificates are valid for **5 years**
- Renewal does **not** change the Team ID (it's tied to the Apple Developer account)
- Keychain ACLs continue to work after renewal without user prompts
- Update the `MACOS_CERTIFICATE` GitHub Secret with the new `.p12`
- Azure Key Vault credentials should be rotated every 90 days

## Alternative: `rcodesign` (Cross-Platform Signing)

The `apple-codesign` Rust crate provides `rcodesign`, an open-source reimplementation of Apple code signing and notarization that runs on Linux. This could allow signing on cheaper Linux runners instead of macOS runners. Still requires the Apple Developer Program membership for the certificate.

Source: https://gregoryszorc.com/blog/2022/08/08/achieving-a-completely-open-source-implementation-of-apple-code-signing-and-notarization/

## Alternative: Homebrew Distribution

`brew install` does not apply the quarantine attribute to binaries, which means Gatekeeper does not check the binary and Keychain prompts are handled differently. This is how `gh`, `ripgrep`, and `rustup` handle macOS distribution without code signing. If Homebrew is the primary distribution channel, code signing becomes less urgent (but still recommended for direct downloads).

## Cost Summary

| Item | Cost | Frequency | Required? |
|------|------|-----------|-----------|
| Apple Developer Program (Individual) | $99 | Annual | Yes (macOS Keychain) |
| Windows OV code signing certificate | $200-500 | Annual | No |
| Azure Key Vault (Premium) | ~$12 | Annual | Only for Windows |
| **Minimum (macOS only)** | **$99** | **Annual** | |
| **Both platforms** | **$310-610** | **Annual** | |

## References

- [Federico Terzi: macOS code signing in GitHub Actions](https://federicoterzi.com/blog/automatic-code-signing-and-notarization-for-macos-apps-using-github-actions/)
- [PaperAge: Notarizing Rust CLI binaries](https://www.randomerrata.com/articles/2024/notarize/)
- [Apple: Notarizing macOS software](https://developer.apple.com/documentation/security/notarizing-macos-software-before-distribution)
- [Tauri: macOS signing docs](https://v2.tauri.app/distribute/sign/macos/)
- [apple-actions/import-codesign-certs](https://github.com/marketplace/actions/import-code-signing-certificates)
- [taiki-e/upload-rust-binary-action](https://github.com/taiki-e/upload-rust-binary-action)
- [Windows code signing with EV cert on GitHub Actions](https://melatonin.dev/blog/how-to-code-sign-windows-installers-with-an-ev-cert-on-github-actions/)
- [Gregory Szorc: Open-source Apple code signing](https://gregoryszorc.com/blog/2022/08/08/achieving-a-completely-open-source-implementation-of-apple-code-signing-and-notarization/)
- [Apple TN2206: Code Signing In Depth](https://developer.apple.com/library/archive/technotes/tn2206/_index.html)
