import akshare as ak

print("Finding Index functions:")
for x in dir(ak):
    if "stock_zh_index_spot" in x:
        print(x)

print("\nFinding Northbound functions:")
for x in dir(ak):
    if "north" in x and "flow" in x:
        print(x)
