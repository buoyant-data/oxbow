use chrono::TimeZone;
use deltalake::parquet::record::Field;
use serde_json::Value as JsonValue;

pub(crate) trait ToMysqlValue {
    fn to_mysql_value(&self) -> Result<String, Box<dyn std::error::Error>>;
}

impl ToMysqlValue for Field {
    fn to_mysql_value(&self) -> Result<String, Box<dyn std::error::Error>> {
        match self {
            Field::Null => Ok("\\N".to_string()),
            Field::Bool(b) => {
                if *b {
                    Ok("1".to_string())
                } else {
                    Ok("0".to_string())
                }
            }

            Field::Byte(n) => Ok(format!("{}", n)),
            Field::Short(n) => Ok(format!("{}", n)),
            Field::Int(n) => Ok(format!("{}", n)),
            Field::Long(n) => Ok(format!("{}", n)),
            Field::UByte(n) => Ok(format!("{}", n)),
            Field::UShort(n) => Ok(format!("{}", n)),
            Field::UInt(n) => Ok(format!("{}", n)),
            Field::ULong(n) => Ok(format!("{}", n)),
            Field::Float16(n) => Ok(format!("{}", n)),
            Field::Float(n) => Ok(format!("{}", n)),
            Field::Double(n) => Ok(format!("{}", n)),

            Field::Str(s) => Ok(s.to_owned()),
            Field::Bytes(_b) => Err("Bytes not supported".into()),

            Field::Date(d) => {
                let date = chrono::Utc
                    .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
                    .latest()
                    .ok_or("Invalid date")?
                    + chrono::Duration::days(*d as i64);
                Ok(date.format("%Y-%m-%d").to_string())
            }
            Field::TimestampMillis(ts) => {
                let ts = chrono::Utc
                    .timestamp_opt(ts / 1_000, (ts % 1_000).try_into().unwrap_or(0) * 1_000_000)
                    .latest()
                    .ok_or("Invalid timestamp")?;
                Ok(ts.format("%Y-%m-%d %H:%M:%S.%6f").to_string())
            }
            Field::TimestampMicros(ts) => {
                let ts = chrono::Utc
                    .timestamp_opt(
                        ts / 1_000_000,
                        (ts % 1_000_000).try_into().unwrap_or(0) * 1_000,
                    )
                    .latest()
                    .ok_or("Invalid timestamp")?;
                Ok(ts.format("%Y-%m-%d %H:%M:%S.%6f").to_string())
            }

            Field::Decimal(_n) => match self.to_json_value() {
                JsonValue::String(s) => Ok(s),
                JsonValue::Number(n) => Ok(n.to_string()),
                _ => Err(format!("Invalid decimal value: {:?}", self))?,
            },

            Field::Group(_row) => Ok(self.to_json_value().to_string()),
            Field::ListInternal(_fields) => Ok(self.to_json_value().to_string()),
            Field::MapInternal(_map) => Ok(self.to_json_value().to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::parquet::data_type::{ByteArray, Decimal};

    #[test]
    fn test_string() {
        let field = Field::Str("hello".to_string());
        assert_eq!(field.to_mysql_value().unwrap(), "hello");
    }

    #[test]
    fn test_null() {
        let field = Field::Null;
        assert_eq!(field.to_mysql_value().unwrap(), "\\N");
    }

    #[test]
    fn test_bool() {
        let field = Field::Bool(true);
        assert_eq!(field.to_mysql_value().unwrap(), "1");

        let field = Field::Bool(false);
        assert_eq!(field.to_mysql_value().unwrap(), "0");
    }

    #[test]
    fn test_int() {
        let field = Field::Int(42);
        assert_eq!(field.to_mysql_value().unwrap(), "42");
        let field = Field::Long(42);
        assert_eq!(field.to_mysql_value().unwrap(), "42");
        let field = Field::Short(42);
        assert_eq!(field.to_mysql_value().unwrap(), "42");
    }

    #[test]
    fn test_decimal() {
        let value = ByteArray::from(vec![207, 200]);
        let field = Field::Decimal(Decimal::from_bytes(value, 8, 2));
        assert_eq!(field.to_mysql_value().unwrap(), "-123.44");

        let value = ByteArray::from(vec![0, 0, 0, 0, 0, 4, 147, 224]);
        let field = Field::Decimal(Decimal::from_bytes(value, 17, 5));
        assert_eq!(field.to_mysql_value().unwrap(), "3.00000");
    }

    #[test]
    fn test_date() {
        let field = Field::Date(20168);
        assert_eq!(field.to_mysql_value().unwrap(), "2025-03-21");
    }

    #[test]
    fn test_timestamp() {
        let field = Field::TimestampMillis(1742564430765);
        assert_eq!(
            field.to_mysql_value().unwrap(),
            "2025-03-21 13:40:30.765000"
        );
        let field = Field::TimestampMillis(1742564430265);
        assert_eq!(
            field.to_mysql_value().unwrap(),
            "2025-03-21 13:40:30.265000"
        );

        let field = Field::TimestampMicros(1742564430765432);
        assert_eq!(
            field.to_mysql_value().unwrap(),
            "2025-03-21 13:40:30.765432"
        );
        let field = Field::TimestampMicros(1742564430265432);
        assert_eq!(
            field.to_mysql_value().unwrap(),
            "2025-03-21 13:40:30.265432"
        );
    }
}
