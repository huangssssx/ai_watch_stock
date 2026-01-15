import akshare as ak

print("\nFinding HSGT functions:")
for x in dir(ak):
    if "hsgt" in x:
        print(x)
