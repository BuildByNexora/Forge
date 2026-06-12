use crate::ForgeError;

pub fn parse_delay(value: &str) -> Result<u64, ForgeError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ForgeError::InvalidDuration(
            "duration string must not be empty".into(),
        ));
    }
    let (num_str, unit) = if let Some(s) = value.strip_suffix("ms") {
        (s, "ms")
    } else if let Some(s) = value.strip_suffix('s') {
        (s, "s")
    } else if let Some(s) = value.strip_suffix('m') {
        (s, "m")
    } else if let Some(s) = value.strip_suffix('h') {
        (s, "h")
    } else if let Some(s) = value.strip_suffix('d') {
        (s, "d")
    } else {
        return Err(ForgeError::InvalidDuration(format!(
            "invalid duration format: {value:?}; expected e.g. 10s, 5m, 1h, 7d"
        )));
    };
    let num: u64 = num_str.parse().map_err(|_| {
        ForgeError::InvalidDuration(format!("invalid number in duration: {value:?}"))
    })?;
    let seconds = match unit {
        "ms" => num.checked_div(1000).unwrap_or(0).max(1),
        "s" => num,
        "m" => num
            .checked_mul(60)
            .ok_or_else(|| ForgeError::InvalidDuration(format!("duration overflow: {value:?}")))?,
        "h" => num
            .checked_mul(3600)
            .ok_or_else(|| ForgeError::InvalidDuration(format!("duration overflow: {value:?}")))?,
        "d" => num
            .checked_mul(86400)
            .ok_or_else(|| ForgeError::InvalidDuration(format!("duration overflow: {value:?}")))?,
        _ => unreachable!(),
    };
    Ok(seconds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_durations() {
        assert_eq!(parse_delay("10s").unwrap(), 10);
        assert_eq!(parse_delay("5m").unwrap(), 300);
        assert_eq!(parse_delay("1h").unwrap(), 3600);
        assert_eq!(parse_delay("7d").unwrap(), 604800);
        assert_eq!(parse_delay("0s").unwrap(), 0);
    }

    #[test]
    fn parse_invalid_durations() {
        assert!(parse_delay("").is_err());
        assert!(parse_delay("abc").is_err());
        assert!(parse_delay("10x").is_err());
    }
}
