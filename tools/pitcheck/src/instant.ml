let micros_per_second = 1_000_000
let micros_per_day = 86_400 * micros_per_second

let floor_div a b = if a >= 0 then a / b else -(((-a) + b - 1) / b)

let days_of_civil y m d =
  let y = if m <= 2 then y - 1 else y in
  let era = floor_div y 400 in
  let yoe = y - (era * 400) in
  let mp = if m > 2 then m - 3 else m + 9 in
  let doy = ((153 * mp) + 2) / 5 + d - 1 in
  let doe = (yoe * 365) + (yoe / 4) - (yoe / 100) + doy in
  (era * 146097) + doe - 719468

let civil_of_days days =
  let z = days + 719468 in
  let era = floor_div z 146097 in
  let doe = z - (era * 146097) in
  let yoe = (doe - (doe / 1460) + (doe / 36524) - (doe / 146096)) / 365 in
  let doy = doe - ((365 * yoe) + (yoe / 4) - (yoe / 100)) in
  let mp = ((5 * doy) + 2) / 153 in
  let d = doy - (((153 * mp) + 2) / 5) + 1 in
  let m = if mp < 10 then mp + 3 else mp - 9 in
  (yoe + (era * 400) + (if m <= 2 then 1 else 0), m, d)

let days_of_iso_date s =
  match String.split_on_char '-' s with
  | [ y; m; d ] -> days_of_civil (int_of_string y) (int_of_string m) (int_of_string d)
  | _ -> invalid_arg (Printf.sprintf "not a date: %S" s)

let at_hour days hour = (days * micros_per_day) + (hour * 3600 * micros_per_second)

let iso micros =
  let days = floor_div micros micros_per_day in
  let rest = micros - (days * micros_per_day) in
  let y, m, d = civil_of_days days in
  let seconds = rest / micros_per_second and fraction = rest mod micros_per_second in
  let clock =
    Printf.sprintf "%04d-%02d-%02dT%02d:%02d:%02d" y m d (seconds / 3600) (seconds / 60 mod 60)
      (seconds mod 60)
  in
  (if fraction = 0 then clock else Printf.sprintf "%s.%06d" clock fraction) ^ "+00:00"
