package math

// Max returns the largest value of provided arguments
func Max(x, y uint64) uint64 {

	if x > y {
		return x
	}

	return y
}

// Min returns the lowest value of provided arguments
func Min(x, y uint64) uint64 {

	if x < y {
		return x
	}

	return y
}

// MinInt returns the lowest value of provided arguments
func MinInt(x, y int64) int64 {

	if x < y {
		return x
	}

	return y
}
