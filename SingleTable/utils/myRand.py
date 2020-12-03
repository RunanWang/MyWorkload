import random

def number(minimum, maximum):
    value = random.randint(minimum, maximum)
    assert minimum <= value and value <= maximum
    return value

def randomString(minimum_length, maximum_length, base, numCharacters):
    length = number(minimum_length, maximum_length)
    baseByte = ord(base)
    string = ""
    for i in range(length):
        string += chr(baseByte + number(0, numCharacters-1))
    return string

def randomCity(city):
    value = random.randint(0, len(city)-1)
    return city[value]

if __name__ == "__main__":
    print(number(1,100))
    print(randomString(1,30,'A',26))
    city = ['BJ', 'SH', 'GZ', 'SZ', 'HB', 'CC', 'SY', 'TJ']
    print(randomCity(city))