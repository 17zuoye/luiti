#-*-coding:utf-8-*-



class MathUtils:

    @staticmethod
    def percent(a, b):
        # reset other False type obj to 0, e.g. None.
        if not b: b = 0
        if not a: a = 0

        if b == 0:
            return 0.0
        result = a / float(b)
        return result

        # 注释原因: 实际存储还是用高精度吧 from @连华
        # return int(round(result * 10000)) / 10000.0
