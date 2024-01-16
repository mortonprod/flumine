find . -exec bunzip2 {} \;

find ../../../data/betfair/soccer/BASIC/2022 -regex '.*\(/[^/]*\)\1' -delete