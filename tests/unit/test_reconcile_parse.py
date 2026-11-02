"""Parsing of user-provided exports: aliases, bad rows, exact money, limits."""

from __future__ import annotations

import json
from datetime import timezone

import pytest

from reconcile_tool.parse import (
    MAX_ROWS,
    InputError,
    parse_gateway,
    parse_merchant,
    to_paise,
)


# --------------------------------------------------------------------------- #
# money: exact, never floats
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    "text,unit,paise",
    [
        ("1200.50", "rupees", 120050),
        ("1,200.50", "rupees", 120050),
        ("₹1,200.50", "rupees", 120050),
        ("Rs. 99", "rupees", 9900),
        ("INR 0.07", "rupees", 7),
        ("19.99", "rupees", 1999),       # float math gives 1998.9999999999998
        ("1.15", "rupees", 115),         # float math gives 114.99999999999999
        ("0.29", "rupees", 29),          # float math gives 28.999999999999996
        ("4.35", "rupees", 435),
        ("120050", "paise", 120050),
        ("0", "paise", 0),
    ],
)
def test_amounts_convert_to_exact_integer_paise(text, unit, paise):
    assert to_paise(text, unit) == paise
    assert isinstance(to_paise(text, unit), int)


def test_amounts_never_pass_through_float_arithmetic():
    # 0.1 + 0.2 != 0.3 in floats; as paise it must be exactly 10 + 20.
    assert to_paise("0.1", "rupees") + to_paise("0.2", "rupees") == to_paise("0.3", "rupees") == 30


@pytest.mark.parametrize(
    "text,unit",
    [
        ("12.345", "rupees"),     # sub-paisa
        ("100.5", "paise"),       # fractional paise
        ("-5", "rupees"),
        ("abc", "rupees"),
        ("1e3", "rupees"),
        ("1,2", "rupees"),        # ambiguous comma
        ("NaN", "rupees"),
        ("", "rupees"),
    ],
)
def test_bad_amounts_are_rejected_with_a_reason(text, unit):
    with pytest.raises(ValueError):
        to_paise(text, unit)


def test_json_numbers_keep_their_exact_decimal_text():
    res = parse_gateway(json.dumps([{"order_id": "A", "status": "captured", "amount": 19.99}]))
    assert res.rows[0].amount == 1999


# --------------------------------------------------------------------------- #
# headers, statuses, defaults
# --------------------------------------------------------------------------- #
def test_header_aliases_and_status_synonyms_in_gateway_csv():
    csv_text = "Razorpay Order ID,Payment Status,Total,Currency Code\nA1,success,100,inr\n"
    res = parse_gateway(csv_text)
    assert res.invalid == []
    row = res.rows[0]
    assert (row.order_id, row.status, row.amount, row.currency) == ("A1", "captured", 10000, "INR")


def test_currency_defaults_to_inr_and_optional_columns_are_read():
    csv_text = (
        "order_id,status,amount,payment_id,refund_id,event_time\n"
        "A1,captured,10,pay_1,,2026-11-01T10:00:00Z\n"
    )
    row = parse_gateway(csv_text).rows[0]
    assert row.currency == "INR" and row.payment_id == "pay_1" and row.refund_id is None
    assert row.event_time.tzinfo is not None and row.event_time.utcoffset().total_seconds() == 0


@pytest.mark.parametrize(
    "raw,canonical",
    [("success", "captured"), ("Paid", "captured"), ("authorised", "authorized"),
     ("Failed", "failed"), ("declined", "failed"), ("pending", "created"), ("refund", "refunded")],
)
def test_gateway_status_synonyms(raw, canonical):
    res = parse_gateway(f"order_id,status,amount\nA,{raw},1\n")
    assert res.rows[0].status == canonical


@pytest.mark.parametrize(
    "raw,canonical",
    [("completed", "paid"), ("processing", "paid"), ("on-hold", "pending"),
     ("Canceled", "cancelled"), ("failed", "failed"), ("refunded", "refunded")],
)
def test_merchant_status_synonyms(raw, canonical):
    res = parse_merchant(f"order_id,status\nA,{raw}\n")
    assert res.rows[0].status == canonical


def test_refund_or_dispute_id_makes_a_reversal_row_regardless_of_status_text():
    res = parse_gateway("order_id,status,amount,refund_id,dispute_id\nA,processed,10,rfnd_1,\nB,open,5,,dp_9\n")
    assert [(r.status, r.refund_id, r.dispute_id) for r in res.rows] == [
        ("refunded", "rfnd_1", None), ("disputed", None, "dp_9"),
    ]


def test_merchant_amount_and_currency_are_optional():
    res = parse_merchant("order_id,status\nA,paid\n")
    assert res.rows[0].amount is None and res.rows[0].currency is None
    res = parse_merchant("order_id,status,amount,currency\nA,paid,5.50,usd\n")
    assert (res.rows[0].amount, res.rows[0].currency) == (550, "USD")


def test_amount_unit_changes_the_conversion():
    assert parse_gateway("order_id,status,amount\nA,captured,500\n", "paise").rows[0].amount == 500
    assert parse_gateway("order_id,status,amount\nA,captured,500\n", "rupees").rows[0].amount == 50000


def test_semicolon_and_tab_delimiters_and_bom_are_handled():
    assert parse_merchant("﻿order_id;status\nA;paid\n").rows[0].status == "paid"
    assert parse_merchant("order_id\tstatus\nA\tpaid\n").rows[0].order_id == "A"


# --------------------------------------------------------------------------- #
# bad rows are reported, never dropped
# --------------------------------------------------------------------------- #
def test_every_bad_row_is_reported_with_its_row_number_and_reasons():
    csv_text = (
        "order_id,status,amount\n"          # row 1 (header)
        "A1,captured,10\n"                  # row 2 ok
        "A2,bogus,10\n"                     # row 3 bad status
        ",captured,10\n"                    # row 4 missing order id
        "A4,captured,12.345\n"              # row 5 sub-paisa
        "A5,captured,\n"                    # row 6 missing amount
        "A6,,5,\n"                          # row 7 missing status
    )
    res = parse_gateway(csv_text)
    assert [r.order_id for r in res.rows] == ["A1"]
    by_row = {i.row: i for i in res.invalid}
    assert sorted(by_row) == [3, 4, 5, 6, 7]
    assert "not recognized" in by_row[3].reasons[0]
    assert by_row[4].reasons == ("order_id is missing",)
    assert "decimal places" in by_row[5].reasons[0]
    assert by_row[6].reasons == ("amount is missing",)
    assert by_row[7].reasons == ("status is missing",)
    assert len(res.rows) + len(res.invalid) == 6    # nothing silently lost


def test_a_row_with_several_problems_lists_all_of_them():
    res = parse_gateway("order_id,status,amount,currency\n,nope,x1,EURO\n")
    assert len(res.invalid[0].reasons) == 4


def test_blank_lines_are_skipped_but_row_numbers_stay_physical():
    res = parse_gateway("order_id,status,amount\n\nA,bogus,1\n")
    assert res.invalid[0].row == 3


def test_json_rows_are_numbered_by_position_and_non_objects_reported():
    res = parse_merchant(json.dumps([{"order_id": "A", "status": "paid"}, "oops", {"order_id": "B", "status": "??"}]))
    assert [r.order_id for r in res.rows] == ["A"]
    assert sorted(i.row for i in res.invalid) == [2, 3]


def test_json_object_wrapping_a_list_is_accepted():
    res = parse_merchant(json.dumps({"orders": [{"order_id": "A", "status": "paid"}]}))
    assert len(res.rows) == 1


# --------------------------------------------------------------------------- #
# whole-input errors and limits
# --------------------------------------------------------------------------- #
def test_missing_required_column_names_the_columns_found():
    with pytest.raises(InputError) as exc:
        parse_gateway("order_id,state\nA,captured\n")
    msg = str(exc.value)
    assert "amount" in msg and "order_id, state" in msg


@pytest.mark.parametrize("text", ["", "   \n", "[not json"])
def test_empty_or_broken_input_is_an_input_error(text):
    with pytest.raises(InputError):
        parse_merchant(text)


def test_row_limit_is_enforced():
    ok = "order_id,status\n" + "".join(f"O{i},paid\n" for i in range(MAX_ROWS))
    assert len(parse_merchant(ok).rows) == MAX_ROWS
    too_many = ok + "ONE_MORE,paid\n"
    with pytest.raises(InputError) as exc:
        parse_merchant(too_many)
    assert str(MAX_ROWS) in str(exc.value)


def test_row_limit_is_enforced_for_json_too():
    rows = [{"order_id": f"O{i}", "status": "paid"} for i in range(MAX_ROWS + 1)]
    with pytest.raises(InputError):
        parse_merchant(json.dumps(rows))
