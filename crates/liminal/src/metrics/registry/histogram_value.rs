pub trait HistogramValue: Copy {
    #[must_use]
    fn into_f64(self) -> f64;
}

impl HistogramValue for f64 {
    fn into_f64(self) -> f64 {
        self
    }
}

impl HistogramValue for f32 {
    fn into_f64(self) -> f64 {
        f64::from(self)
    }
}

macro_rules! impl_lossless_histogram_value {
    ($($value_type:ty),+ $(,)?) => {
        $(
            impl HistogramValue for $value_type {
                fn into_f64(self) -> f64 {
                    f64::from(self)
                }
            }
        )+
    };
}

impl_lossless_histogram_value!(u8, u16, u32, i8, i16, i32);

impl HistogramValue for u64 {
    fn into_f64(self) -> f64 {
        u64_to_f64(self)
    }
}

impl HistogramValue for usize {
    fn into_f64(self) -> f64 {
        let value = u64::try_from(self).unwrap_or(u64::MAX);
        u64_to_f64(value)
    }
}

impl HistogramValue for i64 {
    fn into_f64(self) -> f64 {
        i64_to_f64(self)
    }
}

impl HistogramValue for isize {
    fn into_f64(self) -> f64 {
        let value = i64::try_from(self).unwrap_or_else(|_error| {
            if self.is_negative() {
                i64::MIN
            } else {
                i64::MAX
            }
        });
        i64_to_f64(value)
    }
}

fn u64_to_f64(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from(value & u64::from(u32::MAX)).unwrap_or(u32::MAX);

    f64::from(high).mul_add(4_294_967_296.0, f64::from(low))
}

fn i64_to_f64(value: i64) -> f64 {
    if value.is_negative() {
        -u64_to_f64(value.unsigned_abs())
    } else {
        u64_to_f64(value.unsigned_abs())
    }
}
