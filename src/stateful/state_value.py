class StateValue:

    def __init__(self, value, date, change_func):
        self.value = value
        self.date = date
        self._change_func = change_func

    def __add__(self, other):
        return self.value + other

    def __radd__(self, other):
        return self.value + other

    def __iadd__(self, other):
        self._change_func(self.value + other, self.date)

    def __sub__(self, other):
        return self.value - other

    def __rsub__(self, other):
        return other - self.value

    def __isub__(self, other):
        self._change_func(self.value - other, self.date)

    def __mul__(self, other):
        return self.value * other

    def __rmul__(self, other):
        return other * self.value

    def __imul__(self, other):
        self._change_func(self.value * other, self.date)

    def __div__(self, other):
        return self.value / other

    def __rdiv__(self, other):
        return other / self.value

    def __idiv__(self, other):
        self._change_func(self.value / other, self.date)

    def __eq__(self, other):
        return self.value == other

    def __str__(self):
        return str(self.value)

    def __int__(self):
        return int(self.value)

    def __float__(self):
        return float(self.value)

    def __bool__(self):
        return bool(self.value)

    def __repr__(self):
        return f"StateValue({self.value}, {self.dtype})"
