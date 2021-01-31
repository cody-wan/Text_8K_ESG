import multiprocessing
import os 

def func(x):
    pass

NUM = 1200

# %%time 
A = [func(x) for x in range(NUM)]
len(A)

# %%time 
A = list()
for x in range(NUM):
    A.append(func(x))
len(A)

# %%time 
A = list()
def mycallback(x):
    A.extend(x)
with multiprocessing.Pool(os.cpu_count()) as pool:
    r = pool.map_async(func, (i for i in range(NUM)), callback=mycallback) # return elements ordered
    r.wait()
len(A)

# %%time 
A = list()
results = list()
pool = multiprocessing.Pool(os.cpu_count())
for x in (i for i in range(NUM)):
    r = pool.apply_async(func, (x,), callback=mycallback) # return elements not ordered
    results.append(r)
for r in results:
    r.wait()
len(A)

# parse command line arguments
# parser = argparse.ArgumentParser(description="this script processes every filing document's body text via a "
#                                              "user-defined function/algorithm if a model indicates buy/positive "
#                                              "signal for a document, its CIK and document release date are saved "
#                                              "locally")
# parser.add_argument('MODEL')
# parser.add_argument('MODEL_NAME')
# parser.add_argument('MODEL_PARAM')
# parser.add_argument('-T', '--TEST_FLAG', help='run backtest with a portion of data as test',
#                     nargs="?",
#                     const=True,
#                     default=False)
# args = parser.parse_args()
# """ hyper-parameters """
# # MODEL: name of repository where model results are saved
# # MODEL_NAME: name of the function to call for executing a given model
# # MODEL_PARAM: parameters for model, e.g. key words to detect for keyword-detecting-based model
# MODEL, MODEL_NAME, MODEL_PARAM, TEST_FLAG = args.MODEL, args.MODEL_NAME, args.MODEL_PARAM, args.TEST_FLAG
# MODEL_PARAM = MODEL_PARAM.split(",")