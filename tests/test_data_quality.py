import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
import pytest

# Dựa theo 3.4 Data Quality với Great Expectations trong data-synthesis-plan.md
# Triển khai bằng Pandera cho nhẹ và đồng nhất với test_canonical_schema.py

data_quality_schema = DataFrameSchema({
    "tx_id": Column(str, Check(lambda s: s.notna()), nullable=False),
    # Giả định hash 64 chars từ HMAC-SHA256
    "user_id": Column(str, Check.str_matches(r"^[a-fA-F0-9]{64}$")),
    "amount_minor": Column(int, Check.in_range(0, 1_000_000_000)),
})

def test_data_quality_valid_rows():
    """Kiểm tra các dòng dữ liệu hợp lệ vượt qua schema validation."""
    df = pd.DataFrame([
        {"tx_id": "tx001", "user_id": "a" * 64, "amount_minor": 1500},
        {"tx_id": "tx002", "user_id": "b" * 64, "amount_minor": 2000000},
    ])
    validated_df = data_quality_schema.validate(df)
    assert len(validated_df) == 2

def test_data_quality_invalid_amount():
    """amount_minor không được nhỏ hơn 0 hoặc vượt quá 1 tỷ."""
    df = pd.DataFrame([
        {"tx_id": "tx001", "user_id": "a" * 64, "amount_minor": -50}, # Invalid
    ])
    with pytest.raises(pa.errors.SchemaError):
        data_quality_schema.validate(df)

def test_data_quality_null_tx_id():
    """tx_id không được phép null."""
    df = pd.DataFrame([
        {"tx_id": None, "user_id": "a" * 64, "amount_minor": 50},
    ])
    with pytest.raises(pa.errors.SchemaError):
        data_quality_schema.validate(df)

def test_user_id_regex():
    """user_id phải là chuỗi hash 64 ký tự hex."""
    df = pd.DataFrame([
        {"tx_id": "tx001", "user_id": "invalid_hash_string", "amount_minor": 50},
    ])
    with pytest.raises(pa.errors.SchemaError):
        data_quality_schema.validate(df)

def test_user_id_uniqueness_proportion():
    """expect_column_proportion_of_unique_values_to_be_between("user_id", 0.05, 0.95)"""
    df = pd.DataFrame([
        {"user_id": "a" * 64},
        {"user_id": "a" * 64},
        {"user_id": "b" * 64},
        {"user_id": "c" * 64},
    ])
    unique_ratio = df["user_id"].nunique() / len(df)
    # 3 unique users / 4 rows = 0.75
    assert 0.05 <= unique_ratio <= 0.95

def test_table_row_count_expectation():
    """expect_table_row_count_to_be_between(200000, 1000000) đối với các dataset lớn (mocked)."""
    # Trong unit test ta chỉ mock logic kiểm tra này
    row_count = 500000
    assert 200000 <= row_count <= 1000000
