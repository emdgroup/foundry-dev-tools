from foundry_dev_tools.utils.misc import is_dataset_a_view


def test_is_dataset_a_view():
    assert is_dataset_a_view({"record": {"view": True}})
    assert not is_dataset_a_view({"record": {"view": False}})
    assert not is_dataset_a_view({"transaction": {"rid": "", "record": {"view": True}}})
    assert not is_dataset_a_view({"view": True})
