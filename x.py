class Tokenizer:
    def __init__(self, statement):
        self._statement = statement
        self._idx = 0

    def __iter__(self):
        self._idx = 0

    def __next__(self):
        if self._idx >= len(self._statement):
            raise StopIteration
        token = self._statement[self._idx]
        self._idx += 1
        return token

    def push_back(self):
        self._idx -= 1


t = Tokenizer([1,2,3,4,5])
a = next(t)
print(a)
a = next(t)
print(a)
a = next(t)
print(a)
a = next(t)
print(a)
t.push_back()
a = next(t)
print(a)
a = next(t)
print(a)
a = next(t)
print(a)
