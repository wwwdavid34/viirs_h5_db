#!/usr/bin/env python3

import random
import string


def randomword(length):
    return ''.join(random.choice(string.ascii_uppercase) for _ in range(length))
