# iport asyncio
# import jsonimport match
# import o
# import time
# import traceback
# fromcollections import deque
# from dataclasses import dataclass, field
# from datetime import datetime,timezone
# from statisticsimport median_low


# import websockets
# from auth import get_ws_url
# from signal_core import infer_market_bias

# try: #optional speed_up: pip install orjson import orjson
#     loads= orjson.loads
#     def dumps (obj)
#         return orjson.dums(obj).decode()   #deriv expects text frames
# except importError:
#     loads=json.loads
#     dumps=json.dumps

#     # -_-_-__---_-_-__________--_--_--_-----_-------
#     # configuration 
#     #
# ACCOUNT_TYPE= "demo"
# SYMBOLS=  ("1hz100v")
# CURRENCY="USD"
# STREAK=3# target sreak length(virtual,includes look_agead ticks)
# BIAS_WINDOW=1#ticks measured after the (virtual)streak completes


# LOOKAGEAD=1 #ticks assumed to continue the streak (0=legacy behaviour)
# AUTO_LOOKAHEAD= false# tune LOOKAHEAD from measured entry lag
# MAX_LOKAHEAD=3
# # per direction decision gaurds(0/0.0 =same behaviour as before)
# MIN_SAMPLES=10  # min decided samples in the current direction's bucket
# MIN_EDGE = 0.05  # min gaap between reversal and contination rate(0.05=5pts)



# #(SEGMENT) Sliding stats segment(per direction)
# SEGMENT_ENABLED=True
# SEGMENT_LIMIT=10# when decided samplesin a direction reachthis