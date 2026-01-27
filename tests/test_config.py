"""Tests for exchange configuration system."""

from src.reference_data.adapters.config import (
    BINANCE_CONFIG,
    BITGET_CONFIG,
    BYBIT_CONFIG,
    COINBASE_CONFIG,
    COINBASE_INTL_CONFIG,
    OKX_CONFIG,
    BinanceConfig,
    EndpointConfig,
    ExchangeConfig,
    InstrumentEndpoints,
    OKXConfig,
)
from src.reference_data.api.models import InstrumentType


class TestEndpointConfig:
    """Test EndpointConfig dataclass."""

    def test_endpoint_config_creation(self):
        """Test that EndpointConfig can be created with rest and ws URLs."""
        endpoint = EndpointConfig(
            rest_url="https://api.example.com/v1/info",
            ws_url="wss://stream.example.com/v1/ws",
        )

        assert endpoint.rest_url == "https://api.example.com/v1/info"
        assert endpoint.ws_url == "wss://stream.example.com/v1/ws"

    def test_endpoint_config_with_empty_ws_url(self):
        """Test that EndpointConfig can be created with empty ws_url for REST-only endpoints."""
        endpoint = EndpointConfig(rest_url="https://api.example.com/products", ws_url="")

        assert endpoint.rest_url == "https://api.example.com/products"
        assert endpoint.ws_url == ""


class TestInstrumentEndpoints:
    """Test InstrumentEndpoints dataclass."""

    def test_instrument_endpoints_with_all_types(self):
        """Test InstrumentEndpoints with all three instrument types."""
        endpoints = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://api.example.com/spot",
                ws_url="wss://stream.example.com/spot",
            ),
            perpetual=EndpointConfig(
                rest_url="https://api.example.com/perp",
                ws_url="wss://stream.example.com/perp",
            ),
            futures=EndpointConfig(
                rest_url="https://api.example.com/futures",
                ws_url="wss://stream.example.com/futures",
            ),
        )

        assert endpoints.spot is not None
        assert endpoints.perpetual is not None
        assert endpoints.futures is not None
        assert endpoints.spot.rest_url == "https://api.example.com/spot"
        assert endpoints.perpetual.rest_url == "https://api.example.com/perp"
        assert endpoints.futures.rest_url == "https://api.example.com/futures"

    def test_instrument_endpoints_with_partial_support(self):
        """Test InstrumentEndpoints with only some instrument types."""
        endpoints = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://api.example.com/spot", ws_url="wss://ws.example.com"
            ),
            perpetual=None,
            futures=None,
        )

        assert endpoints.spot is not None
        assert endpoints.perpetual is None
        assert endpoints.futures is None

    def test_instrument_endpoints_default_none(self):
        """Test that InstrumentEndpoints defaults to None for all fields."""
        endpoints = InstrumentEndpoints()

        assert endpoints.spot is None
        assert endpoints.perpetual is None
        assert endpoints.futures is None


class TestExchangeConfig:
    """Test ExchangeConfig dataclass."""

    def test_exchange_config_creation(self):
        """Test basic ExchangeConfig creation."""
        mainnet = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://api.example.com/spot", ws_url="wss://ws.example.com"
            )
        )
        testnet = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://testnet.example.com/spot",
                ws_url="wss://testnet.example.com/ws",
            )
        )

        config = ExchangeConfig(
            mainnet=mainnet,
            testnet=testnet,
            rest_rate_limit=10,
            ping_timeout=30.0,
        )

        assert config.mainnet == mainnet
        assert config.testnet == testnet
        assert config.rest_rate_limit == 10
        assert config.ping_timeout == 30.0
        assert config.rest_rate_period == 1.0  # default
        assert config.ws_rate_limit == 5  # default
        assert config.ws_rate_period == 1.0  # default
        assert config.ping_interval == 15.0  # default

    def test_get_endpoint_mainnet(self):
        """Test get_endpoint returns correct mainnet endpoint."""
        mainnet = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://api.example.com/spot", ws_url="wss://ws.example.com"
            )
        )
        testnet = InstrumentEndpoints()

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        endpoint = config.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.example.com/spot"
        assert endpoint.ws_url == "wss://ws.example.com"

    def test_get_endpoint_testnet(self):
        """Test get_endpoint returns correct testnet endpoint."""
        mainnet = InstrumentEndpoints()
        testnet = InstrumentEndpoints(
            spot=EndpointConfig(
                rest_url="https://testnet.example.com/spot",
                ws_url="wss://testnet.example.com/ws",
            )
        )

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        endpoint = config.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://testnet.example.com/spot"
        assert endpoint.ws_url == "wss://testnet.example.com/ws"

    def test_get_endpoint_returns_none_for_unsupported(self):
        """Test get_endpoint returns None for unsupported instrument types."""
        mainnet = InstrumentEndpoints(spot=EndpointConfig(rest_url="url", ws_url="ws"))
        testnet = InstrumentEndpoints()

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        endpoint = config.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is None

    def test_get_supported_instruments_mainnet(self):
        """Test get_supported_instruments returns correct list for mainnet."""
        mainnet = InstrumentEndpoints(
            spot=EndpointConfig(rest_url="url1", ws_url="ws1"),
            perpetual=EndpointConfig(rest_url="url2", ws_url="ws2"),
        )
        testnet = InstrumentEndpoints()

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        supported = config.get_supported_instruments(is_testnet=False)

        assert len(supported) == 2
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES not in supported

    def test_get_supported_instruments_testnet(self):
        """Test get_supported_instruments returns correct list for testnet."""
        mainnet = InstrumentEndpoints()
        testnet = InstrumentEndpoints(spot=EndpointConfig(rest_url="url1", ws_url="ws1"))

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        supported = config.get_supported_instruments(is_testnet=True)

        assert len(supported) == 1
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL not in supported
        assert InstrumentType.FUTURES not in supported

    def test_get_supported_instruments_empty(self):
        """Test get_supported_instruments returns empty list when no instruments supported."""
        mainnet = InstrumentEndpoints()
        testnet = InstrumentEndpoints()

        config = ExchangeConfig(mainnet=mainnet, testnet=testnet)

        supported = config.get_supported_instruments(is_testnet=False)

        assert len(supported) == 0
        assert supported == []


class TestBinanceConfig:
    """Test BINANCE_CONFIG constant."""

    def test_binance_config_has_all_instrument_types_mainnet(self):
        """Test that Binance supports spot, perpetual, and futures on mainnet."""
        supported = BINANCE_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 3
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    def test_binance_config_has_all_instrument_types_testnet(self):
        """Test that Binance supports spot, perpetual, and futures on testnet."""
        supported = BINANCE_CONFIG.get_supported_instruments(is_testnet=True)

        assert len(supported) == 3
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    def test_binance_config_mainnet_spot_endpoint(self):
        """Test Binance mainnet spot endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.binance.com/api/v3/exchangeInfo"
        assert endpoint.ws_url == "wss://ws-api.binance.com:443/ws-api/v3"

    def test_binance_config_mainnet_perpetual_endpoint(self):
        """Test Binance mainnet perpetual endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://fapi.binance.com/fapi/v1/exchangeInfo"
        assert endpoint.ws_url == "wss://fstream.binance.com/ws"

    def test_binance_config_mainnet_futures_endpoint(self):
        """Test Binance mainnet futures endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://dapi.binance.com/dapi/v1/exchangeInfo"
        assert endpoint.ws_url == "wss://dstream.binance.com/ws"

    def test_binance_config_testnet_spot_endpoint(self):
        """Test Binance testnet spot endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://testnet.binance.vision/api/v3/exchangeInfo"
        assert endpoint.ws_url == "wss://testnet.binance.vision/ws-api/v3"

    def test_binance_config_testnet_perpetual_endpoint(self):
        """Test Binance testnet perpetual endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://testnet.binancefuture.com/fapi/v1/exchangeInfo"
        assert endpoint.ws_url == "wss://stream.binancefuture.com/ws"

    def test_binance_config_testnet_futures_endpoint(self):
        """Test Binance testnet futures endpoint."""
        endpoint = BINANCE_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://testnet.binancefuture.com/dapi/v1/exchangeInfo"
        assert endpoint.ws_url == "wss://dstream.binancefuture.com/ws"

    def test_binance_config_rate_limits(self):
        """Test Binance config rate limits."""
        assert BINANCE_CONFIG.rest_rate_limit == 10
        assert BINANCE_CONFIG.rest_rate_period == 1.0
        assert BINANCE_CONFIG.ws_rate_limit == 5
        assert BINANCE_CONFIG.ws_rate_period == 1.0
        assert BINANCE_CONFIG.ping_interval == 15.0
        assert BINANCE_CONFIG.ping_timeout == 55.0


class TestOKXConfig:
    """Test OKX_CONFIG constant."""

    def test_okx_config_has_all_instrument_types_mainnet(self):
        """Test that OKX supports spot, perpetual, and futures on mainnet."""
        supported = OKX_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 3
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    def test_okx_config_mainnet_spot_endpoint(self):
        """Test OKX mainnet spot endpoint."""
        endpoint = OKX_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        assert endpoint.ws_url == "wss://ws.okx.com:8443/ws/v5/public"

    def test_okx_config_mainnet_perpetual_endpoint(self):
        """Test OKX mainnet perpetual endpoint."""
        endpoint = OKX_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
        assert endpoint.ws_url == "wss://ws.okx.com:8443/ws/v5/public"

    def test_okx_config_mainnet_futures_endpoint(self):
        """Test OKX mainnet futures endpoint."""
        endpoint = OKX_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://www.okx.com/api/v5/public/instruments?instType=FUTURES"
        assert endpoint.ws_url == "wss://ws.okx.com:8443/ws/v5/public"

    def test_okx_config_testnet_uses_wspap_domain(self):
        """Test that OKX testnet uses wspap.okx.com for WebSocket."""
        endpoint = OKX_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert endpoint is not None
        assert "wspap.okx.com" in endpoint.ws_url

    def test_okx_config_ping_timeout(self):
        """Test OKX config ping timeout."""
        assert OKX_CONFIG.ping_timeout == 30.0


class TestBybitConfig:
    """Test BYBIT_CONFIG constant."""

    def test_bybit_config_has_all_instrument_types_mainnet(self):
        """Test that Bybit supports spot, perpetual, and futures on mainnet."""
        supported = BYBIT_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 3
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    def test_bybit_config_mainnet_spot_endpoint(self):
        """Test Bybit mainnet spot endpoint."""
        endpoint = BYBIT_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.bybit.com/v5/market/instruments-info?category=spot"
        assert endpoint.ws_url == "wss://stream.bybit.com/v5/public/spot"

    def test_bybit_config_mainnet_perpetual_endpoint(self):
        """Test Bybit mainnet perpetual endpoint."""
        endpoint = BYBIT_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is not None
        assert (
            endpoint.rest_url == "https://api.bybit.com/v5/market/instruments-info?category=linear"
        )
        assert endpoint.ws_url == "wss://stream.bybit.com/v5/public/linear"

    def test_bybit_config_mainnet_futures_endpoint(self):
        """Test Bybit mainnet futures endpoint."""
        endpoint = BYBIT_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is not None
        assert (
            endpoint.rest_url == "https://api.bybit.com/v5/market/instruments-info?category=inverse"
        )
        assert endpoint.ws_url == "wss://stream.bybit.com/v5/public/inverse"

    def test_bybit_config_testnet_uses_testnet_domain(self):
        """Test that Bybit testnet uses api-testnet.bybit.com and stream-testnet.bybit.com."""
        endpoint = BYBIT_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert endpoint is not None
        assert "api-testnet.bybit.com" in endpoint.rest_url
        assert "stream-testnet.bybit.com" in endpoint.ws_url


class TestBitgetConfig:
    """Test BITGET_CONFIG constant."""

    def test_bitget_config_has_all_instrument_types_mainnet(self):
        """Test that Bitget supports spot, perpetual, and futures on mainnet."""
        supported = BITGET_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 3
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.FUTURES in supported

    def test_bitget_config_mainnet_spot_endpoint(self):
        """Test Bitget mainnet spot endpoint."""
        endpoint = BITGET_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.bitget.com/api/v2/spot/public/symbols"
        assert endpoint.ws_url == "wss://ws.bitget.com/v2/ws/public"

    def test_bitget_config_mainnet_perpetual_endpoint(self):
        """Test Bitget mainnet perpetual endpoint."""
        endpoint = BITGET_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is not None
        assert (
            endpoint.rest_url
            == "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"
        )
        assert endpoint.ws_url == "wss://ws.bitget.com/v2/ws/public"

    def test_bitget_config_mainnet_futures_endpoint(self):
        """Test Bitget mainnet futures endpoint."""
        endpoint = BITGET_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is not None
        assert (
            endpoint.rest_url
            == "https://api.bitget.com/api/v2/mix/market/contracts?productType=COIN-FUTURES"
        )
        assert endpoint.ws_url == "wss://ws.bitget.com/v2/ws/public"

    def test_bitget_config_testnet_same_as_mainnet(self):
        """Test that Bitget testnet uses same endpoints as mainnet."""
        mainnet_endpoint = BITGET_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)
        testnet_endpoint = BITGET_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert mainnet_endpoint is not None
        assert testnet_endpoint is not None
        assert mainnet_endpoint.rest_url == testnet_endpoint.rest_url
        assert mainnet_endpoint.ws_url == testnet_endpoint.ws_url


class TestCoinbaseConfig:
    """Test COINBASE_CONFIG constant."""

    def test_coinbase_config_has_spot_only(self):
        """Test that Coinbase supports spot only."""
        supported = COINBASE_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 1
        assert InstrumentType.SPOT in supported
        assert InstrumentType.PERPETUAL not in supported
        assert InstrumentType.FUTURES not in supported

    def test_coinbase_config_perpetual_is_none(self):
        """Test that Coinbase perpetual endpoint is None."""
        endpoint = COINBASE_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is None

    def test_coinbase_config_futures_is_none(self):
        """Test that Coinbase futures endpoint is None."""
        endpoint = COINBASE_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is None

    def test_coinbase_config_mainnet_spot_endpoint(self):
        """Test Coinbase mainnet spot endpoint."""
        endpoint = COINBASE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.exchange.coinbase.com/products"
        assert endpoint.ws_url == ""

    def test_coinbase_config_testnet_spot_endpoint(self):
        """Test Coinbase testnet spot endpoint."""
        endpoint = COINBASE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api-public.sandbox.exchange.coinbase.com/products"
        assert endpoint.ws_url == ""

    def test_coinbase_config_has_empty_ws_url(self):
        """Test that Coinbase has empty ws_url (REST polling only)."""
        endpoint = COINBASE_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is not None
        assert endpoint.ws_url == ""


class TestCoinbaseIntlConfig:
    """Test COINBASE_INTL_CONFIG constant."""

    def test_coinbase_intl_config_has_perpetual_only(self):
        """Test that Coinbase International supports perpetual only."""
        supported = COINBASE_INTL_CONFIG.get_supported_instruments(is_testnet=False)

        assert len(supported) == 1
        assert InstrumentType.PERPETUAL in supported
        assert InstrumentType.SPOT not in supported
        assert InstrumentType.FUTURES not in supported

    def test_coinbase_intl_config_spot_is_none(self):
        """Test that Coinbase International spot endpoint is None."""
        endpoint = COINBASE_INTL_CONFIG.get_endpoint(InstrumentType.SPOT, is_testnet=False)

        assert endpoint is None

    def test_coinbase_intl_config_futures_is_none(self):
        """Test that Coinbase International futures endpoint is None."""
        endpoint = COINBASE_INTL_CONFIG.get_endpoint(InstrumentType.FUTURES, is_testnet=False)

        assert endpoint is None

    def test_coinbase_intl_config_mainnet_perpetual_endpoint(self):
        """Test Coinbase International mainnet perpetual endpoint."""
        endpoint = COINBASE_INTL_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=False)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api.international.coinbase.com/api/v1/instruments"
        assert endpoint.ws_url == "wss://ws-feed.international.coinbase.com"

    def test_coinbase_intl_config_testnet_perpetual_endpoint(self):
        """Test Coinbase International testnet perpetual endpoint."""
        endpoint = COINBASE_INTL_CONFIG.get_endpoint(InstrumentType.PERPETUAL, is_testnet=True)

        assert endpoint is not None
        assert endpoint.rest_url == "https://api-n5e1.coinbase.com/api/v1/instruments"
        assert endpoint.ws_url == "wss://ws-feed-n5e1.coinbase.com"


class TestLegacyBinanceConfig:
    """Test legacy BinanceConfig class for backward compatibility."""

    def test_legacy_binance_config_mainnet(self):
        """Test legacy BinanceConfig with mainnet."""
        config = BinanceConfig(is_testnet=False)

        assert config.rest_url == "https://api.binance.com/api/v3/exchangeInfo"
        assert config.ws_url == "wss://ws-api.binance.com:443/ws-api/v3"
        assert config.rest_rate_limit == 10
        assert config.rest_rate_period == 1.0
        assert config.ws_rate_limit == 5
        assert config.ws_rate_period == 1.0
        assert config.ping_interval == 15.0
        assert config.ping_timeout == 55.0

    def test_legacy_binance_config_testnet(self):
        """Test legacy BinanceConfig with testnet."""
        config = BinanceConfig(is_testnet=True)

        assert config.rest_url == "https://testnet.binance.vision/api/v3/exchangeInfo"
        assert config.ws_url == "wss://testnet.binance.vision/ws-api/v3"


class TestLegacyOKXConfig:
    """Test legacy OKXConfig class for backward compatibility."""

    def test_legacy_okx_config_mainnet(self):
        """Test legacy OKXConfig with mainnet."""
        config = OKXConfig(is_testnet=False)

        assert config.rest_url == "https://www.okx.com/api/v5/public/instruments?instType=SPOT"
        assert config.ws_url == "wss://ws.okx.com:8443/ws/v5/public"
        assert config.rest_rate_limit == 5
        assert config.rest_rate_period == 1.0
        assert config.ws_rate_limit == 5
        assert config.ws_rate_period == 1.0
        assert config.ping_interval == 15.0
        assert config.ping_timeout == 30.0

    def test_legacy_okx_config_testnet(self):
        """Test legacy OKXConfig with testnet."""
        config = OKXConfig(is_testnet=True)

        assert "wspap.okx.com" in config.ws_url
