import khayyam as kym


def jalali_to_gregorian(x, format='%Y-%m-%d'):
    if not isinstance(x, str):
        raise TypeError('Input date should be a string.')
    return kym.JalaliDate.strptime(x, format).todate()