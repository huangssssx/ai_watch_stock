
import akshare as ak
import inspect

def find_lhb_apis():
    print("Searching for LHB APIs in akshare...")
    for name, obj in inspect.getmembers(ak):
        if "lhb" in name.lower() and inspect.isfunction(obj):
            print(f"- {name}")
            try:
                sig = inspect.signature(obj)
                print(f"  Signature: {sig}")
            except:
                pass

if __name__ == "__main__":
    find_lhb_apis()
