import random

SYLLABLES = ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"]

nurand_var = None  # NURand


def setNURand(nu):
    global nurand_var
    nurand_var = nu


def NURand(a, x, y):
    """A non-uniform random number, as defined by TPC-C 2.1.6. (page 20)."""
    global nurand_var
    assert x <= y
    if nurand_var is None:
        setNURand(makeForLoad())

    if a == 255:
        c = nurand_var.cLast
    elif a == 1023:
        c = nurand_var.cId
    elif a == 8191:
        c = nurand_var.orderLineItemId
    else:
        raise Exception("a = " + a + " is not a supported value")

    return (((number(0, a) | number(x, y)) + c) % (y - x + 1)) + x


def number(minimum, maximum):
    value = random.randint(minimum, maximum)
    assert minimum <= value and value <= maximum
    return value


def numberExcluding(minimum, maximum, excluding):
    """An in the range [minimum, maximum], excluding excluding."""
    assert minimum < maximum
    assert minimum <= excluding and excluding <= maximum

    # Generate 1 less number than the range
    num = number(minimum, maximum - 1)

    # Adjust the numbers to remove excluding
    if num >= excluding: num += 1
    assert minimum <= num and num <= maximum and num != excluding
    return num


def fixedPoint(decimal_places, minimum, maximum):
    assert decimal_places > 0
    assert minimum < maximum

    multiplier = 1
    for i in range(0, decimal_places):
        multiplier *= 10

    int_min = int(minimum * multiplier + 0.5)
    int_max = int(maximum * multiplier + 0.5)

    return float(number(int_min, int_max) / float(multiplier))


## DEF

def selectUniqueIds(numUnique, minimum, maximum):
    rows = set()
    for i in range(0, numUnique):
        index = None
        while index == None or index in rows:
            index = number(minimum, maximum)

        rows.add(index)

    assert len(rows) == numUnique
    return rows


def astring(minimum_length, maximum_length):
    """A random alphabetic string with length in range [minimum_length, maximum_length]."""
    return randomString(minimum_length, maximum_length, 'a', 26)


def nstring(minimum_length, maximum_length):
    """A random numeric string with length in range [minimum_length, maximum_length]."""
    return randomString(minimum_length, maximum_length, '0', 10)


def randomString(minimum_length, maximum_length, base, numCharacters):
    length = number(minimum_length, maximum_length)
    baseByte = ord(base)
    string = ""
    for i in range(length):
        string += chr(baseByte + number(0, numCharacters - 1))
    return string


def makeLastName(number):
    """A last name as defined by TPC-C 4.3.2.3. Not actually random."""
    global SYLLABLES
    assert 0 <= number and number <= 999
    indicies = [int(number / 100), int((number / 10) % 10), number % 10]
    return "".join(map(lambda x: SYLLABLES[x], indicies))


def makeRandomLastName(maxCID):
    """A non-uniform random last name, as defined by TPC-C 4.3.2.3. The name will be limited to maxCID."""
    min_cid = 999
    if (maxCID - 1) < min_cid: min_cid = maxCID - 1
    return makeLastName(NURand(255, 0, min_cid))


def makeForLoad():
    """Create random NURand constants, appropriate for loading the database."""
    cLast = number(0, 255)
    cId = number(0, 1023)
    orderLineItemId = number(0, 8191)
    return NURandC(cLast, cId, orderLineItemId)


def validCRun(cRun, cLoad):
    """Returns true if the cRun value is valid for running. See TPC-C 2.1.6.1 (page 20)"""
    cDelta = abs(cRun - cLoad)
    return 65 <= cDelta and cDelta <= 119 and cDelta != 96 and cDelta != 112


def makeForRun(loadC):
    """Create random NURand constants for running TPC-C. TPC-C 2.1.6.1. (page 20) specifies the valid range for these constants."""
    cRun = number(0, 255)
    while validCRun(cRun, loadC.cLast) == False:
        cRun = number(0, 255)
    assert validCRun(cRun, loadC.cLast)

    cId = number(0, 1023)
    orderLineItemId = number(0, 8191)
    return NURandC(cRun, cId, orderLineItemId)


def rand_bool(prob):
    temp_rand_int = random.randint(1, 100)
    if temp_rand_int <= prob:
        return True
    else:
        return False


class NURandC:
    def __init__(self, cLast, cId, orderLineItemId):
        self.cLast = cLast
        self.cId = cId
        self.orderLineItemId = orderLineItemId
