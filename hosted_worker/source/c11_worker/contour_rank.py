ATR='ORIGINAL_ARMED_TRAIL_INTRABAR'
def ranks(original,op,line,low,old_floor):
 if low>line or line<=old_floor:return False
 if original is None:return True
 return bool(original['why']==ATR and original['clock']=='INTRABAR_IDEAL' and op>line>original['price'])
