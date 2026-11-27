class State:
 def __init__(self):self.fast=self.slow=self.signal=self.diff=None;self.n=0
 def preview(self,c):
  if self.fast is None:return c,c,0.,0.
  f=self.fast+(c-self.fast)/3.;s=self.slow+2*(c-self.slow)/35.;e=100*(f/s-1);sig=self.signal+(e-self.signal)/3.
  return f,s,e,sig
 def flags(self,c):
  f,s,e,sig=self.preview(c);d=e-sig
  return self.diff is not None and d>0 and self.diff<=0,self.diff is not None and d<0 and self.diff>=0,e,sig
 def commit(self,c):
  self.fast,self.slow,e,self.signal=self.preview(c);self.diff=e-self.signal;self.n+=1
