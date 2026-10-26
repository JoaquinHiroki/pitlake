let shortest_scientific x =
  let rec go p =
    let s = Printf.sprintf "%.*e" p x in
    if p >= 16 || float_of_string s = x then s else go (p + 1)
  in
  go 0

let float_repr x =
  if not (Float.is_finite x) then invalid_arg "float_repr: JSON has no NaN or infinity";
  if x = 0.0 then if 1.0 /. x < 0.0 then "-0.0" else "0.0"
  else
    let s = shortest_scientific (Float.abs x) in
    let at = String.index s 'e' in
    let digits = String.concat "" (String.split_on_char '.' (String.sub s 0 at)) in
    let exponent =
      let e = String.sub s (at + 1) (String.length s - at - 1) in
      int_of_string (if e.[0] = '+' then String.sub e 1 (String.length e - 1) else e)
    in
    let n = String.length digits in
    let body =
      if exponent >= 16 || exponent < -4 then
        let mantissa =
          if n = 1 then digits else String.sub digits 0 1 ^ "." ^ String.sub digits 1 (n - 1)
        in
        Printf.sprintf "%se%c%02d" mantissa (if exponent < 0 then '-' else '+') (abs exponent)
      else if exponent >= 0 then
        if n <= exponent + 1 then digits ^ String.make (exponent + 1 - n) '0' ^ ".0"
        else
          String.sub digits 0 (exponent + 1)
          ^ "."
          ^ String.sub digits (exponent + 1) (n - exponent - 1)
      else "0." ^ String.make (-exponent - 1) '0' ^ digits
    in
    (if x < 0.0 then "-" else "") ^ body

let string_literal s =
  let b = Buffer.create (String.length s + 2) in
  Buffer.add_char b '"';
  String.iter
    (fun c ->
      match c with
      | '"' -> Buffer.add_string b "\\\""
      | '\\' -> Buffer.add_string b "\\\\"
      | '\n' -> Buffer.add_string b "\\n"
      | '\r' -> Buffer.add_string b "\\r"
      | '\t' -> Buffer.add_string b "\\t"
      | c when Char.code c < 0x20 || Char.code c > 0x7e ->
          Buffer.add_string b (Printf.sprintf "\\u%04x" (Char.code c))
      | c -> Buffer.add_char b c)
    s;
  Buffer.add_char b '"';
  Buffer.contents b
