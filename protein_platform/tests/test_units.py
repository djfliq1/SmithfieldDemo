import pytest
from app.orchestration import NormalizationError, to_lb


def test_to_lb_lb_passthrough():
    result = to_lb(100.0, "LB")
    assert result == 100.0
    assert isinstance(result, float)


def test_to_lb_lb_case_insensitive():
    result = to_lb(50.0, "lb")
    assert result == 50.0


def test_to_lb_kg_conversion():
    result = to_lb(1.0, "KG")
    assert abs(result - 2.2046226218) < 1e-9


def test_to_lb_kg_case_insensitive():
    result = to_lb(1.0, "kg")
    assert abs(result - 2.2046226218) < 1e-9


def test_to_lb_invalid_uom_raises():
    with pytest.raises(NormalizationError) as exc_info:
        to_lb(10.0, "OZ")
    assert "OZ" in str(exc_info.value)


def test_to_lb_invalid_uom_metric_ton():
    with pytest.raises(NormalizationError):
        to_lb(1.0, "MT")
