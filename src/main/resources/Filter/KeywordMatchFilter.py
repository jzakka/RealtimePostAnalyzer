import sys

data = sys.argv[1]

bad_words = ["시발", "씨발", "개새끼", "새끼", "ㅈㄴ", "염병", "옘병", "쉬불", "느금"]

for bad_word in bad_words:
    if bad_word in data:
        print("dangerous")
        sys.exit()

print("safe")