class Zigzag:

    @staticmethod
    def zigzag_encode(x):
        return x << 1 if x >= 0 else -2 * x - 1

    @staticmethod
    def zigzag_decode(x):
        return x >> 1 if x & 1 == 0 else (x + 1) / -2

    @staticmethod
    def varint_encode(x):
        mask = 0x7f
        rv = x & mask
        x >>= 7
        i = 1
        while x:
            rv |= (x & mask | 0x80) << (i * 8)
            x >>= 7
            i += 1
        return rv

    @staticmethod
    def varint_decode(x):
        mask = 0x7f
        rv = 0
        i = 0
        while x:
            rv |= (x & mask) << (7 * i)
            i += 1
            x >>= 8
        return rv
