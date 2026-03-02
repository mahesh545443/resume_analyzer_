def check_reverse(words):
    result = {}
    word_set = set(words)   # for fast lookup

    for word in words:
        if word[::-1] in word_set:
            result[word] = True
        else:
            result[word] = False

    return result

a = ["dog", "god"]
b = ["apple", "orange"]

print(check_reverse(a))
print(check_reverse(b))


