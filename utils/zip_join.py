from operator import itemgetter
from decimal import Decimal


class JoinIterator(object):
    def __init__(self, seq, fill_val):
        self.iterator = iter(seq)
        self.fillvalue = fill_val
        self.fill_row = None
        self.done = False

    def step(self):
        try:
            if not self.fill_row:
                row = next(self.iterator)
                self.fill_row = (self.fillvalue,) * len(row)
                return row
            return next(self.iterator)
        except StopIteration:
            self.done = True
            if not self.fill_row:
                raise Exception('empty sequence!')
            return self.fill_row


def zip_join(left, right, *args, **kwargs):
    if not args:
        raise Exception('join clause not provided!')
    key = itemgetter(*args)
    fillvalue = kwargs.get('fillvalue', Decimal(0))

    i_left = JoinIterator(left, fillvalue)
    i_right = JoinIterator(right, fillvalue)

    left_row = i_left.step()
    right_row = i_right.step()

    while not i_left.done and not i_right.done:
        if key(left_row) == key(right_row):
            yield (left_row, right_row)
            left_row = i_left.step()
            right_row = i_right.step()
        elif key(left_row) > key(right_row):
            yield (i_left.fill_row, right_row)
            right_row = i_right.step()
        elif key(left_row) < key(right_row):
            yield (left_row, i_right.fill_row)
            left_row = i_left.step()

    if i_left.done:
        while not i_right.done:
            yield (i_left.fill_row, right_row)
            right_row = i_right.step()
    elif i_right.done:
        while not i_left.done:
            yield (left_row, i_right.fill_row)
            left_row = i_left.step()
