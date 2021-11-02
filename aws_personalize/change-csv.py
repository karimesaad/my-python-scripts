import csv 

inputFileName1 = "views01-06.csv"
inputFileName2 = "views07-12.csv"
outputFileName = "views_modified.csv"

with open(inputFileName1, 'rb') as inFile1, open(inputFileName2, 'rb') as inFile2, open(outputFileName, 'wb') as outfile:
    r1 = csv.reader(inFile1)
    r2 = csv.reader(inFile2)
    w = csv.writer(outfile)

    # lines1= len(list(r1))
    # print("size of views 1: ")
    # print(lines1)
    # lines1= len(list(r2))
    # print("size of views 2: ")
    # print(lines1)

    next(r1, None)  # skip the first row from the reader, the old header
    next(r2, None)  # skip the first row from the reader, the old header
    # write new header
    w.writerow(['USER_ID', 'ITEM_ID', 'TIMESTAMP', 'EVENT_TYPE', 'EVENT_VALUE'])

    # copy the rest
    for row in r1:
        w.writerow(row)
    
    for row in r2:
        w.writerow(row)


# readOutFileName = "views_modified.csv"

# with open(readOutFileName, 'rb') as inFile1:
#     r1 = csv.reader(inFile1)

#     counter = 0
#     for row in r1:
#         print(row)
#         counter = counter + 1
#         if counter == 6:
#             break


        

        