// Parser for the competition's LF-only, delimiter-free field values.
#pragma once
#include <array>
#include <cstdint>
#include <limits>
#include <string_view>
using Text = std::string_view;
using Time = int64_t;
constexpr Time day_ms = 86400000;
constexpr Time bad_time = std::numeric_limits<Time>::min();

template <size_t N>
bool fields(Text line, std::array<Text, N>& out) {
  for (size_t i = 0; i < N; ++i) {
    size_t sep = line.find(',');
    if ((sep == Text::npos) != (i + 1 == N)) return false;
    Text value = line.substr(0, sep);
    if (!value.empty() && value.front() == '"') {
      if (value.size() < 2 || value.back() != '"') return false;
      value.remove_prefix(1);
      value.remove_suffix(1);
    }
    if (value.find('"') != Text::npos) return false;
    out[i] = value;
    if (sep != Text::npos) line.remove_prefix(sep + 1);
  }
  return true;
}

inline Time decimal(Text s, Time limit) {
  if (s.empty()) return bad_time;
  Time value = 0;
  for (unsigned char ch : s) {
    unsigned digit = unsigned(ch) - '0';
    if (digit > 9 || value > (limit - digit) / 10) return bad_time;
    value = value * 10 + digit;
  }
  return value;
}

inline int two(const char* s) {
  unsigned a = static_cast<unsigned char>(s[0]) - unsigned('0');
  unsigned b = static_cast<unsigned char>(s[1]) - unsigned('0');
  return (a > 9 || b > 9) ? -10000 : int(a * 10 + b);
}

inline Time timestamp(Text s) {
  if (!((s.size() == 20 || s.size() == 25) && s[4] == '-'))
    return decimal(s, INT64_MAX);
  if (s[7] != '-' || s[10] != 'T' || s[13] != ':' || s[16] != ':')
    return bad_time;
  const char* p = s.data();
  int y = two(p) * 100 + two(p + 2), m = two(p + 5), d = two(p + 8);
  int hour = two(p + 11), minute = two(p + 14), sec = two(p + 17);
  if (y < 1 || m < 1 || m > 12 || d < 1 || hour < 0 || hour > 23 ||
      minute < 0 || minute > 59 || sec < 0 || sec > 59)
    return bad_time;
  constexpr int lengths[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  bool leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
  if (d > lengths[m - 1] + (m == 2 && leap)) return bad_time;
  int offset = 0;
  if (s.size() == 20) {
    if (s[19] != 'Z') return bad_time;
  } else {
    if ((s[19] != '+' && s[19] != '-') || s[22] != ':') return bad_time;
    int h = two(p + 20), n = two(p + 23);
    if (h < 0 || h > 23 || n < 0 || n > 59) return bad_time;
    offset = (h * 60 + n) * (s[19] == '+' ? 1 : -1);
  }
  // Gregorian day number with March as month zero.
  y -= m <= 2;
  int era = y / 400, yr = y % 400;
  int days = era * 146097 + yr * 365 + yr / 4 - yr / 100 +
             (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1 - 719468;
  return days * day_ms + ((hour * 60 + minute - offset) * 60 + sec) * 1000LL;
}
