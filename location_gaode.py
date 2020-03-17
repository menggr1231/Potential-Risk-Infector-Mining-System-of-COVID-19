import requests

# return the latitude and longitude information of the corresponding address, for example：
# In: address("兰州西站")
# Out: 103.752739,36.068391
class convert_address(object):
    def __init__(self, filename, output_filename):
        self.filename = filename
        self.output_filename = output_filename
        self.ak = 'c1f4c84541f5385c9f3c016c0767443d'
        file = open(self.filename, encoding='UTF-8')
        for line in file.readlines():
            curLine = line.strip().split(" ")
            loc1 = self.address(curLine[2])  # return the latitude and longitude of curLine[2]
            loc1 = str(loc1.split(",")[0]) + " " + str(loc1.split(",")[1])
            loc2 = self.address(curLine[4])  # return the latitude and longitude of curLine[4]
            loc2 = str(loc2.split(",")[0]) + " " + str(loc2.split(",")[1])
            # Express time as hours starting at 00:00 on January 23
            # We only considered January and February
            if curLine[3].split(".")[0] == "1":
                t1 = (int(curLine[3].split(".")[1]) - 23) * 24 \
                     + (int(curLine[3].split(".")[2]) - 0) * 1
            elif curLine[3].split(".")[0] == "2":
                t1 = (31 - 23 + 1) * 24 \
                     + (int(curLine[3].split(".")[1]) - 1) * 24 \
                     + (int(curLine[3].split(".")[2]) - 0) * 1
            if curLine[5].split(".")[0] == "1":
                t2 = (int(curLine[5].split(".")[1]) - 23) * 24 \
                     + (int(curLine[5].split(".")[2]) - 0) * 1
            elif curLine[5].split(".")[0] == "2":
                t2 = (31 - 23 + 1) * 24 \
                     + (int(curLine[5].split(".")[1]) - 1) * 24 \
                     + (int(curLine[5].split(".")[2]) - 0) * 1
            resulttxt = curLine[0] + " " + curLine[1] + " " + loc1 + " " + str(t1) + " " + loc2 + " " + str(t2)
            with open(self.output_filename, 'a', encoding="utf-8") as file_handle:
                file_handle.write(resulttxt)
                file_handle.write('\n')

    def address(self, address):
        url = "http://restapi.amap.com/v3/geocode/geo?key=%s&address=%s" % (self.ak, address)
        data = requests.get(url)
        contest = data.json()
        contest = contest['geocodes'][0]['location']
        return contest
