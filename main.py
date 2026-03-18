import requests
import string

API_URL = "https://api.pi.delivery/v1/pi"

def fetch_pi_digits(start: int, count: int) -> str:
    r = requests.get(API_URL, params={
        "start": start,
        "numberOfDigits": count,
    }, timeout=30)
    # print(r.text)
    r.raise_for_status()
    return r.json()["content"]

def encode_message(msg: str) -> list:
    enc_chars = []
    alpha_dict = {c: i for (i, c) in enumerate(string.ascii_uppercase)}
    for char in msg.upper():
        enc_chars.append(alpha_dict[char])
    return enc_chars

# out = encode_message("hello")
# print(out, type(out))

def mod_search_digits(target: list, digits: str) -> int:
    limit = len(digits) - (2 * len(target)) - 1
    for i in range(limit):
        ok = True
        for j, want in enumerate(target):
            block = int(digits[i + 2*j : i + 2*j + 2])
            if block % 26 != want:
                ok = False
                break
        if ok:
            return i

def main(msg: str, batch_size: int = 1000):
    enc_msg = encode_message(msg)
    overlap = 2 * len(enc_msg) - 1

    start = 1
    tail = ""

    while True:
        digits = tail + fetch_pi_digits(start, batch_size)
        search = mod_search_digits(enc_msg, digits)
        if search:
            return start - len(tail) + search
        tail = digits[-overlap:] if overlap > 0 else ""
        start += batch_size
        print(f"Checked up to digit {start}...")

# print(fetch_pi_digits(0, 1000)[-50:])
message = "Loveumom"
enc = encode_message(message)
result = main(message)
raw_enc = "".join([f"{i:02}" for i in enc])
print(f"Encoded message is {raw_enc}")
print(f"Found at {result}")
print(f"Digits are {fetch_pi_digits(result, len(raw_enc))}")