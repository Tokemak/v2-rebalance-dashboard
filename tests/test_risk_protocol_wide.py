# import os
# import pytest
# from mainnet_launch.pages.protocol_wide import PROTOCOL_CONTENT_FUNCTIONS


# os.environ.setdefault("STREAMLIT_HEADLESS", "1")


# def _run_page(_fn_name: str):
#     from mainnet_launch.pages.protocol_wide import PROTOCOL_CONTENT_FUNCTIONS as _fns

#     fn = _fns[_fn_name]
#     fn()


# def _param_generator():
#     for fn_name, _ in PROTOCOL_CONTENT_FUNCTIONS.items():
#         yield pytest.param(
#             fn_name,
#             {},
#             id=f"{fn_name}",
#         )


# @pytest.mark.parametrize("fn_name,kwargs", list(_param_generator()))
# def test_protocol_wide_pages(fn_name, kwargs):
#     _run_page(**kwargs, _fn_name=fn_name)
