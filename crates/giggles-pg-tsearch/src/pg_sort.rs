// Port of PostgreSQL 18.6 lib/sort_template.h for sorts whose ordering of
// equal entries is observable. See NOTICE for the NetBSD BSD-3-Clause license.
// Copyright (c) 2021-2025, PostgreSQL Global Development Group.
// Copyright (c) 1992, 1993, The Regents of the University of California.
use std::cmp::Ordering;

fn median<T>(data: &[T], cmp: &impl Fn(&T, &T) -> Ordering, a: usize, b: usize, c: usize) -> usize {
    let compare = |x: usize, y: usize| cmp(&data[x], &data[y]);
    if compare(a, b).is_lt() {
        if compare(b, c).is_lt() {
            b
        } else if compare(a, c).is_lt() {
            c
        } else {
            a
        }
    } else if compare(b, c).is_gt() {
        b
    } else if compare(a, c).is_lt() {
        a
    } else {
        c
    }
}

pub(crate) fn sort<T>(mut data: &mut [T], cmp: &impl Fn(&T, &T) -> Ordering) {
    loop {
        let n = data.len();
        if n < 7 {
            for next in 1..n {
                let mut i = next;
                while i > 0 && cmp(&data[i - 1], &data[i]).is_gt() {
                    data.swap(i - 1, i);
                    i -= 1;
                }
            }
            return;
        }
        if data.is_sorted_by(|a, b| !cmp(a, b).is_gt()) {
            return;
        }

        let mut pivot = n / 2;
        if n > 7 {
            let (mut first, mut last) = (0, n - 1);
            if n > 40 {
                let step = n / 8;
                first = median(data, cmp, first, first + step, first + 2 * step);
                pivot = median(data, cmp, pivot - step, pivot, pivot + step);
                last = median(data, cmp, last - 2 * step, last - step, last);
            }
            pivot = median(data, cmp, first, pivot, last);
        }
        data.swap(0, pivot);

        // Scan inwards while moving values equal to the pivot to the ends.
        // Both right indices are exclusive so they can reach the start safely.
        let (mut equal_left, mut left, mut right, mut equal_right) = (1, 1, n, n);
        loop {
            while left < right {
                let order = cmp(&data[left], &data[0]);
                if order.is_gt() {
                    break;
                }
                if order.is_eq() {
                    data.swap(left, equal_left);
                    equal_left += 1;
                }
                left += 1;
            }
            while left < right {
                let order = cmp(&data[right - 1], &data[0]);
                if order.is_lt() {
                    break;
                }
                right -= 1;
                if order.is_eq() {
                    equal_right -= 1;
                    data.swap(right, equal_right);
                }
            }
            if left == right {
                break;
            }
            right -= 1;
            data.swap(left, right);
            left += 1;
        }

        // Rotate the equal values from the ends into the middle.
        let less = left - equal_left;
        let more = equal_right - right;
        let count = equal_left.min(less);
        for i in 0..count {
            data.swap(i, left - count + i);
        }
        let count = more.min(n - equal_right);
        for i in 0..count {
            data.swap(left + i, n - count + i);
        }

        let (left_data, rest) = data.split_at_mut(less);
        let right_data = &mut rest[n - less - more..];
        if less <= more {
            sort(left_data, cmp);
            data = right_data;
        } else {
            sort(right_data, cmp);
            data = left_data;
        }
    }
}
