# l1=int(input("enter your number"))
# for i in range (2,l1):
#     if l1%i==0:
#         print("not prime")
#         break
# else:
#     print("its a prime number"

def fact(n):
    if n==0 or n==1:
        return 1
    else:
        return n*(fact(n-1))
fa=fact(5)
print(fa)

