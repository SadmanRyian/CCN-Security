import sys, getopt, glob, os, csv, operator
from os import listdir

def main(argv):
	welcome()
	if len(argv) != 6:
		usage()
		sys.exit(2)

	try:
		opts, args = getopt.getopt(argv, 'd:a:o:')
	except getopt.GetoptError as err:
		print(err)
		usage()
		sys.exit(2)

	reportPath = ''
	attributesList = []
	outputCSVName = ''
	fileType = 'MessageStatsReport'

	for op, value in opts:
		if op == '-d':
			reportPath = value
		elif op == '-o':
			outputCSVName = value
		elif op == '-a':
			attributesList = value.split(',')
		else:
			print("Invalid flag:", op)

	os.chdir(reportPath)
	onlyAppFiles = glob.glob('*' + fileType + '*')
	with open(outputCSVName + '.csv', 'w', newline='', encoding='utf-8') as outPutFile:
		attrFileCSV = csv.writer(outPutFile, quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')

		# Write the name of the attributes
		attrNameList = ['scenario']
		for attr in attributesList:
			attrNameList.append(attr)

		scenarioExample = 'Rui\'s_scenario_routing-EpidemicRouter_seed-1_distribution-1_bufferSize-64M_msgTTL-500_c-5_p-10_i-35_PIT-false_' + fileType + '.txt'

		allValue = [attrNameList]
		numOfFileProc = 0
		for attrFile in onlyAppFiles:
			attrFileRow = [attrFile]
			with open(attrFile, 'r', encoding='utf-8', errors='ignore') as tempFile:
				numOfFileProc += 1
				for line in tempFile:
					for attr in attributesList:
						if attr in line:
							parts = line.rstrip().split(': ')
							if len(parts) > 1:
								attrFileRow.append(parts[1])
							break
			allValue.append(attrFileRow)

		attrFileCSV.writerows(allValue)

	# calculate average for different seed
	avgDictionary = {}
	avgDictionaryShowTime = {}
	with open(outputCSVName + ".csv", mode='r', encoding='utf-8', errors='ignore') as totalResultFile:
		totalResultFileReader = csv.reader(totalResultFile, delimiter=',')
		next(totalResultFileReader, None)  # skip header
		for row_ in totalResultFileReader:
			if not row_:
				continue
			row = row_[0].split('_')
			key = ''
			for i in row[4:]:
				if '-' in i:
					key += i + '_'

			if key in avgDictionary:
				avgDictionaryShowTime[key] += 1
				tempSet = avgDictionary[key]
				tempSetResult = [float(x) + float(y) for x, y in zip(tempSet, row_[1:])]
				avgDictionary[key] = tempSetResult
			else:
				avgDictionaryShowTime[key] = 1
				avgDictionary[key] = [float(x) for x in row_[1:]]

	for key in avgDictionary.keys():
		avgDictionary[key] = [float(x) / float(avgDictionaryShowTime[key]) for x in avgDictionary[key]]
		avgDictionary[key] = forScenName(key) + avgDictionary[key]

	routingType = getRoutingType(reportPath)
	with open(outputCSVName + '_D1_AVG.csv', 'w', newline='', encoding='utf-8') as resultFile_D1:
		resultFileCSVReader_D1 = csv.writer(resultFile_D1, quotechar='|', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
		scenarioAttrList = getParametersInScenarioName(scenarioExample)
		totalResultInArray_D1 = [["bufferSize"] + attrNameList[1:] + scenarioAttrList + ['routing']]

		for key in avgDictionary.keys():
			valueInScenario = getValueFromScenario(key, scenarioAttrList)
			totalResultInArray_D1.append([getBufferSize(avgDictionary[key][0])] + avgDictionary[key][1:] + valueInScenario + routingType)

		resultFileCSVReader_D1.writerows(totalResultInArray_D1)

	print(f"{numOfFileProc} file(s) processed. Result stored in {reportPath}{outputCSVName}.csv and {reportPath}{outputCSVName}_D1_AVG.csv")


def getRoutingType(path):
	return path.split('/')[-1:]


def getValueFromScenario(scenarioName, attrListToGet):
	result = []
	for attr in scenarioName.split('_'):
		if '-' in attr and attr.split('-')[0] in attrListToGet:
			result.append(attr.split('-')[1])
	return result


def getParametersInScenarioName(scenario):
	result = []
	for attr in scenario.split('_')[4:]:
		if '-' in attr:
			result.append(attr.split('-')[0])
	return result


def getBufferSize(scenarioName):
	return scenarioName[scenarioName.find("b") + 1 : scenarioName.find("M")]


def forScenName(longName):
	longNameList = longName.split('_')
	longNameList = [i.split('-')[0][0] + i.split('-')[1] for i in longNameList[:-1]]
	result = '_'.join(longNameList)
	return [result]


def welcome():
	print("\n**************************************************")
	print("** Welcome to Application Reporter Extractor    **")
	print("** This program extracts the attributes(-a)     **\n** from all application reports in directory(-d)**\n** to one csv file(-o)                          **")
	print("**************************************************\n")


def usage():
	print("Usage: python extractApplicationReport.py -d reportsDirectory -a attributesList -o fileNamePrefix")
	print("Example: python extractApplicationReport.py -d ../reports/ -a \"query_count,response_count\" -o ApplicationReport")


def done():
	print("Done!")


if __name__ == "__main__":
	main(sys.argv[1:])
	done()
