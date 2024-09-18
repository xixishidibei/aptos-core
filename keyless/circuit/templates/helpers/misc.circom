pragma circom 2.1.3;

include "./arrays.circom";
include "./hashtofield.circom";
include "./packing.circom";
include "circomlib/circuits/gates.circom";
include "circomlib/circuits/bitify.circom";

// Checks if character 'char' is a whitespace character. Returns 1 if so, 0 otherwise
// Assumes char is a valid ascii character. Does not check for non-ascii unicode whitespace chars.
template isWhitespace() {
   signal input char;  
                       
   signal is_tab <== IsEqual()([char, 9]); // character is a tab space

   signal is_line_break_part_1 <== GreaterEqThan(8)([char, 10]); // ASCII bytes values between 10 ...
   signal is_line_break_part_2 <== LessEqThan(8)([char, 13]); //    ... and 13 inclusive are line break characters
   signal is_line_break <== is_line_break_part_1 * is_line_break_part_2;

   signal is_space <== IsEqual()([char, 32]); // ' '
                       
   signal output is_whitespace <== is_tab + is_line_break + is_space;
}

// https://github.com/TheFrozenFire/snark-jwt-verify/blob/master/circuits/calculate_total.circom
// This circuit returns the sum of the inputs.
// n must be greater than 0.
template CalculateTotal(n) {
    signal input nums[n];
    signal output sum;

    signal sums[n];
    sums[0] <== nums[0];

    for (var i=1; i < n; i++) {
        sums[i] <== sums[i - 1] + nums[i];
    }

    sum <== sums[n - 1];
}

// Given input `in`, enforces that `in[0] === in[1]` if `bool` is 1
template AssertEqualIfTrue() {
    signal input in[2];
    signal input bool;

    (in[0]-in[1]) * bool === 0;
}

// Enforce that if uid name is "email", the email verified field is either true or "true"
template EmailVerifiedCheck(maxEVNameLen, maxEVValueLen, maxUIDNameLen) {
    signal input ev_name[maxEVNameLen];
    signal input ev_value[maxEVValueLen];
    signal input ev_value_len;
    signal input uid_name[maxUIDNameLen];
    signal input uid_name_len;
    signal output uid_is_email;

    var email[5] = [101, 109, 97, 105, 108]; // email

    var uid_starts_with_email_0 = IsEqual()([email[0], uid_name[0]]);
    var uid_starts_with_email_1 = IsEqual()([email[1], uid_name[1]]);
    var uid_starts_with_email_2 = IsEqual()([email[2], uid_name[2]]);
    var uid_starts_with_email_3 = IsEqual()([email[3], uid_name[3]]);
    var uid_starts_with_email_4 = IsEqual()([email[4], uid_name[4]]);

    var uid_starts_with_email = MultiAND(5)([uid_starts_with_email_0, uid_starts_with_email_1, uid_starts_with_email_2, uid_starts_with_email_3, uid_starts_with_email_4]);


    signal uid_name_len_is_5 <== IsEqual()([uid_name_len, 5]);
    uid_is_email <== AND()(uid_starts_with_email, uid_name_len_is_5); // '1' if uid_name is "email" with length 5. This guarantees uid_name is in fact "email" (with quotes) combined with the logic in `JWTFieldCheck`

    var required_ev_name[14] = [101, 109, 97, 105, 108, 95, 118, 101, 114, 105, 102, 105, 101, 100];    // email_verified

    // If uid name is "email", enforce ev_name is "email_verified"
    for (var i = 0; i < 14; i++) {
        AssertEqualIfTrue()([ev_name[i], required_ev_name[i]], uid_is_email);
    }

    signal ev_val_len_is_4 <== IsEqual()([ev_value_len, 4]);
    signal ev_val_len_is_6 <== IsEqual()([ev_value_len, 6]);
    var ev_val_len_is_correct = OR()(ev_val_len_is_4, ev_val_len_is_6);

    signal not_uid_is_email <== NOT()(uid_is_email);
    signal is_ok <== OR()(not_uid_is_email, ev_val_len_is_correct);
    is_ok === 1;
    
    var required_ev_val_len_4[4] = [116, 114, 117, 101]; // true
    signal check_ev_val_bool <== AND()(ev_val_len_is_4, uid_is_email);
    for (var i = 0; i < 4; i ++) {
        AssertEqualIfTrue()([required_ev_val_len_4[i], ev_value[i]], check_ev_val_bool);
    }

    var required_ev_val_len_6[6] = [34, 116, 114, 117, 101, 34]; // "true"
    signal check_ev_val_str <== AND()(ev_val_len_is_6, uid_is_email);
    for (var i = 0; i < 6; i++) {
        AssertEqualIfTrue()([required_ev_val_len_6[i], ev_value[i]], check_ev_val_str);
    }
}

// Given an array `in` of ASCII characters of size `len`, outputs an array `out` marking
// the spaces in between quotes, so that the indices in between quotes in `in` are given the value
// `1` in `out`, and are 0 otherwise. I.e. in = He"llo w"orld! -> out = 00011111000000
template StringBodies(len) {
  signal input in[len];
  signal output out[len];


  signal quotes[len];
  signal quote_parity[len];
  signal quote_parity_1[len];
  signal quote_parity_2[len];

  signal backslashes[len];
  signal adjacent_backslash_parity[len];

  quotes[0] <== IsEqual()([in[0], 34]); 
  quote_parity[0] <== IsEqual()([in[0], 34]); 

  backslashes[0] <== IsEqual()([in[0], 92]);
  adjacent_backslash_parity[0] <== IsEqual()([in[0], 92]);

  for (var i = 1; i < len; i++) {
    backslashes[i] <== IsEqual()([in[i], 92]);
    adjacent_backslash_parity[i] <== backslashes[i] * (1 - adjacent_backslash_parity[i-1]);
  }

  for (var i = 1; i < len; i++) {
    var is_quote = IsEqual()([in[i], 34]); 
    var prev_is_odd_backslash = adjacent_backslash_parity[i-1];
    quotes[i] <== is_quote * (1 - prev_is_odd_backslash);
    quote_parity_1[i] <== quotes[i] * (1 - quote_parity[i-1]);
    quote_parity_2[i] <== (1 - quotes[i]) * quote_parity[i-1];
    quote_parity[i] <== quote_parity_1[i] + quote_parity_2[i];
  }


  out[0] <== 0;

  for (var i = 1; i < len; i++) {
    out[i] <== AND()(quote_parity[i-1], quote_parity[i]);
  }
}

// Given an array of ASCII characters `arr`, returns an array `brackets` with
// a 1 in the position of each open bracket `{`, a -1 in the position of each closed bracket `}`
// and 0 everywhere else
template BracketsMap(len) {
    signal input arr[len];
    signal output brackets[len];

    for (var i = 0; i < len; i++) {
        var is_open_bracket = IsEqual()([arr[i], 123]); // 123 = `{`
        var is_closed_bracket = IsEqual()([arr[i], 125]); // 125 = '}'
        brackets[i] <== is_open_bracket + (0-is_closed_bracket);
    }
}

// Given an input array `arr` of length `len`, outputs an array `out` which
// at each index, contains positive integers in between the spaces of open brackets
// which have not been closed, where open brackets are represented by '1', and
// closed brackets are represented by `0`.
// EXCEPTIONS: The index of the second-to-last closed bracket will contain a `0`
// The first character is skipped - in our specific application we always expect an
// open bracket in the first JWT character
template FillBracketsMap(len) {
    signal input arr[len];
    signal output out[len];

    signal prelim_out1[len];
    signal prelim_out2[len];
    prelim_out1[0] <== IsEqual()([arr[0], 1]); // First character is assumed not to be a closed bracket
    for (var i = 1; i < len; i++) {
        var is_open_bracket = IsEqual()([arr[i], 1]);
        var is_closed_bracket = IsEqual()([arr[i], -1]);
        prelim_out1[i] <== is_open_bracket + prelim_out1[i-1] - is_closed_bracket; // Subtracting 1 here amounts to ignoring the outermost open and closed brackets, which is what we want
    }
    for (var i = 0; i < len; i++) {
        prelim_out2[i] <== prelim_out1[i]-1;
    }
    // Remove all negative numbers from the array
    for (var i = 0; i < len; i++) {
        var is_neg = LessThan(20)([prelim_out2[i], 0]);
        out[i] <== prelim_out2[i] * (1-is_neg);
    }
}

// Given a base64-encoded array `in`, max length `maxN`, and actual unpadded length `n`, returns
// the actual length of the decoded string
template Base64DecodedLength(maxN) {
    var max_q = (3 * maxN) \ 4;
    //signal input in[maxN];
    signal input n; // actual lenght
    signal output decoded_len;
    signal q <-- 3*n \ 4;
    signal r <-- 3*n % 4;

    3*n - 4*q - r === 0;
    signal r_correct_reminder <== LessThan(2)([r, 4]);
    r_correct_reminder === 1;

    // use log function to compute log(max_q)
    signal q_correct_quotient <== LessThan(252)([q, max_q]);
    q_correct_quotient === 1;

    // var eq = 61;
    // assumes valid encoding (if last != "=" then second to last is also
    // != "=")
    // TODO: We don't seem to need this, as the jwt spec removes b64 padding
    // see https://datatracker.ietf.org/doc/html/rfc7515#page-54
    //signal l <== SelectArrayValue(maxN)(in, n - 1);
    //signal s2l <== SelectArrayValue(maxN)(in, n - 2);
    //signal s_l <== IsEqual()([l, eq]);
    //signal s_s2l <== IsEqual()([s2l, eq]);
    //signal reducer <== -1*s_l -1*s_s2l;
    //decoded_len <== q + reducer;
    //log("decoded_len", decoded_len);
}
