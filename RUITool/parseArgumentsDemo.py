import sys, csv

first = ['1', '2', '3']
second = ['4', '5', '6']
forth = ['7','8', '9']
third = [first]
third.append(second)
#third.append(zip(*forth))
tempFile = open("temp.txt", 'w')
tempFileCSV = csv.writer(tempFile, delimiter = ',')

print len(third), len(third[0])

tempFileCSV.writerows(third)
tempFile.close()

print third
#print zip(*first).append(zip(*second))
