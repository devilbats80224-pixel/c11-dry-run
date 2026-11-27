from decimal import Decimal

def exact_plan_quantity(plan,validated_margin,price,leverage,step):
 """No epsilon or upward repair. Use only already-approved, unchanged plan."""
 d=lambda x:Decimal(str(x))
 q,m,p,L,s=map(d,(plan['quantity'],plan['resolved_margin'],price,leverage,step))
 if not all(x.is_finite() and x>0 for x in (q,m,p,L,s)):raise ValueError('PLAN_NONFINITE')
 if q%s!=0 or q*p!=m*L:raise ValueError('PLAN_IDENTITY_OR_LOT')
 if validated_margin!=float(m):return None
 f=float(q)
 if not d(f).is_finite() or d(f)!=q:raise ValueError('PLAN_QUANTITY_FLOAT_NOT_EXACT')
 return f
