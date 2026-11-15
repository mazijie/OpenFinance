from datetime import date

import httpx
import pytest

from backend.providers.finance import FinanceProviderError
from backend.providers.tencent_securities import TencentSecurityProvider


def test_rank_returns_stock_codes_and_rejects_other_securities():
    entries = [{"code": "sh600519", "stock_type": "GP-A"},
               {"code": "sz000001", "stock_type": "GP-A"},
               {"code": "sh000001", "stock_type": "ZS"}]
    def handler(req):
        return httpx.Response(200, json={"code": 0, "data": {"offset": 0, "total": 3, "rank_list": entries}})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        provider = TencentSecurityProvider(client=client)
        assert provider.fetch_a_share_symbols_for_day(day=date.today()) == ["000001.SZ", "600519.SH"]


def test_incomplete_page_is_not_silently_accepted():
    def handler(req):
        return httpx.Response(200, json={"code": 0, "data": {"offset": 0, "total": 2, "rank_list": [{"code": "sh600519"}]}})
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(FinanceProviderError):
            TencentSecurityProvider(client=client).fetch_a_share_symbols_for_day(day=date.today())


def test_metadata_is_sourced_from_tencent_without_inventing_dates():
    def handler(req):
        return httpx.Response(200, content='v_sh600519="1~贵州茅台~600519~1500";'.encode("gb18030"))
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        record = TencentSecurityProvider(client=client).fetch_security_master(symbols=["600519.SH"])[0]
    assert (record.name, record.provider, record.list_date, record.delist_date) == ("贵州茅台", "tencent", None, None)
