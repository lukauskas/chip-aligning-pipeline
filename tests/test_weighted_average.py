from decimal import Decimal

from chipalign.signal.bins import _log10_weighted_mean
from hypothesis import given
from hypothesis.strategies import composite, integers, floats, lists
from nose.tools import assert_almost_equal


@composite
def weights_and_values(draw):
    length = draw(integers(min_value=2, max_value=200))

    v = draw(lists(floats(min_value=0, max_value=10000, allow_infinity=False, allow_nan=False), min_size=length,
                   max_size=length))
    w = draw(lists(integers(min_value=1, max_value=200), min_size=length, max_size=length))
    return w, v


@given(weights_and_values())
def test_log10_weighted_average(w_and_v):
    weights, values = w_and_v

    sum_w = Decimal(0)
    sum_v = Decimal(0)

    # Compute weighted average without loss of precision
    for weight, value in zip(weights, values):
        weight = Decimal(weight)
        value = Decimal(value)

        value = Decimal(10.0) ** (-value)
        sum_v += value * weight
        sum_w += weight

    expected_answer = -(sum_v / sum_w).log10()
    expected_answer = float(expected_answer)
    actual_answer = _log10_weighted_mean(zip(values, weights))

    assert_almost_equal(actual_answer, expected_answer)
