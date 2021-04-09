import sqlparse
import csv
import re
import sys
def eval(c1,op1,constant1,row,productcol):
	if(op1==">=" and row[productcol.index(c1)]>=constant1):
		return True
	if(op1=="<=" and row[productcol.index(c1)]<=constant1):
		return True
	if(op1==">" and row[productcol.index(c1)]>constant1):
		return True
	if(op1=="<" and row[productcol.index(c1)]<constant1):
		return True
	if(op1=="=" and row[productcol.index(c1)]==constant1):
		return True
	return False
def cartesian(tables,tablecontent,tablename,tablecontentrow):
	product=tablecontentrow[tablename[0]][:]
	#print(product)	
	for i in range(1,len(tablename)):
		temp=[]		
		for j in range(len(product)):			
			for k in range(len(tablecontentrow[tablename[i]])):		
				temp.append(product[j][:])
				temp[-1].extend(tablecontentrow[tablename[i]][k])
		product=temp[:]	
	#print(product)
	#print(len(product))
	return product
def where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,colmname,aggflg,grpflg):
	if(aggflg==1):
		temp=colmname
	if("DISTINCT" in str(stmt).upper()):
		tokens[2]=tokens[2].replace(" ","")
		colmname=tokens[2].split(",")
		tokens[4]=tokens[4].replace(" ","")
		tablename=tokens[4].split(",")
		tokens[5]=tokens[5].replace(" >= ",">=")
		tokens[5]=tokens[5].replace(" <= ","<=")
		tokens[5]=tokens[5].replace(" = ","=")
		tokens[5]=tokens[5].replace(" > ",">")
		tokens[5]=tokens[5].replace(" < ","<")
		wheretoken=tokens[5].upper()
	else:
		tokens[1]=tokens[1].replace(" ","")
		colmname=tokens[1].split(",")
		tokens[3]=tokens[3].replace(" ","")
		tablename=tokens[3].split(",")
		tokens[4]=tokens[4].replace(" >= ",">=")
		tokens[4]=tokens[4].replace(" <= ","<=")
		tokens[4]=tokens[4].replace(" = ","=")
		tokens[4]=tokens[4].replace(" > ",">")
		tokens[4]=tokens[4].replace(" < ","<")
		wheretoken=tokens[4].upper()
	if(grpflg==0):	
		if(aggflg==1):
			colmname=temp	
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		for j in colmname:
			flg=0
			for i in tablename:
				if(j in tables[i]):
					flg=1
					break
			if(flg==0):
				print("{0} column not present".format(j.lower()))
				exit()
	product=cartesian(tables,tablecontent,tablename,tablecontentrow)
	productcol=[]
	for i in tablename:
		productcol.extend(tables[i])
	#print(productcol)
	wheretoken=wheretoken.split(" ")
	#print(wheretoken)
	if("AND" in wheretoken or "OR" in wheretoken):		
		con1=wheretoken[1]
		con2=wheretoken[3]
		if(con2[-1]==";"):
			con2=con2[:-1]		
		if(">=" in con1):
			c1=con1[:con1.index(">")]
			constant1=int(con1[con1.index("=")+1:])
			op1=">="
		elif("<=" in con1):
			c1=con1[:con1.index("<")]
			constant1=int(con1[con1.index("=")+1:])
			op1="<="
		elif("<" in con1):
			c1=con1[:con1.index("<")]
			constant1=int(con1[con1.index("<")+1:])
			op1="<"
		elif(">" in con1):
			c1=con1[:con1.index(">")]
			constant1=int(con1[con1.index(">")+1:])
			op1=">"
		elif("=" in con1):
			c1=con1[:con1.index("=")]
			constant1=int(con1[con1.index("=")+1:])
			op1="="	
		if(">=" in con2):
			c2=con2[:con2.index(">")]
			constant2=int(con2[con2.index("=")+1:])
			op2=">="
		elif("<=" in con2):
			c2=con2[:con2.index("<")]
			constant2=int(con2[con2.index("=")+1:])
			op2="<="
		elif("<" in con2):
			c2=con2[:con2.index("<")]
			constant2=int(con2[con2.index("<")+1:])
			op2="<"
		elif(">" in con2):
			c2=con2[:con2.index(">")]
			constant2=int(con2[con2.index(">")+1:])
			op2=">"
		elif("=" in con2):
			c2=con2[:con2.index("=")]
			constant2=int(con2[con2.index("=")+1:])
			op2="="		
		#print(c1,constant1,op1)
		#print(c2,constant2,op2)
		#print(wheretoken)
		ans=[]
		if(c1 not in productcol):
			print("{0} column not present".format(c1.lower()))
			exit()
		if(c2 not in productcol):
			print("{0} column not present".format(c2.lower()))
			exit()
		if(wheretoken[2]=="AND"):		
			for i in range(len(product)):
				#print(product[i])
				if((eval(c1,op1,constant1,product[i],productcol) and eval(c2,op2,constant2,product[i],productcol))==True):
					ans.append(product[i])
		if(wheretoken[2]=="OR"):		
			for i in range(len(product)):
				if((eval(c1,op1,constant1,product[i],productcol) or eval(c2,op2,constant2,product[i],productcol))==True):
					ans.append(product[i])
		#print(ans)		
		return ans
	else:
		con1=wheretoken[1]
		if(con1[-1]==";"):
			con1=con1[:-1]
		if(">=" in con1):
			c1=con1[:con1.index(">")]
			constant1=int(con1[con1.index("=")+1:])
			op1=">="
		elif("<=" in con1):
			c1=con1[:con1.index("<")]
			constant1=int(con1[con1.index("=")+1:])
			op1="<="
		elif("<" in con1):
			c1=con1[:con1.index("<")]
			constant1=int(con1[con1.index("<")+1:])
			op1="<"
		elif(">" in con1):
			c1=con1[:con1.index(">")]
			constant1=int(con1[con1.index(">")+1:])
			op1=">"
		elif("=" in con1):
			c1=con1[:con1.index("=")]
			constant1=int(con1[con1.index("=")+1:])
			op1="="
		if(c1 not in productcol):
			print("{0} column not present".format(c1.lower()))
			exit()		
		ans=[]		
		for i in range(len(product)):
			if(eval(c1,op1,constant1,product[i],productcol)==True):
				ans.append(product[i])
		#print(ans,c1,op1,constant1)
		#print(ans)
		return ans
#print(op)
#	if("AND" in str(stmt).upper()):
#		condition1=
def tokenization(queries,tables,tablecontent,tablecontentrow):
	agg=["SUM","AVG","MIN","MAX","COUNT"]
	parsed  = sqlparse.parse(queries)
	#print(queries)    
	stmt = parsed[0]
	#if(str(stmt)[-1]!=";"):
		
	#print(stmt)
	tokens=[]	
	for tkn in stmt.tokens:
		#print("--> " + str(tkn))
		if(str(tkn)!=" "): 
			str1=str(tkn).replace(", ",",").replace(" ,",",")
			tokens.append(str1)
	if(tokens[-1][-1]!=";"):
		print("Invalid query")
		exit()
	#print(tokens)
	if(tokens[0].upper()!="SELECT"):
		print("Invalid Query")
	elif("GROUP BY" in str(stmt).upper()):
		i=0		
		while(1):
			if(tokens[i].upper()=="GROUP BY"):
				break
			i+=1
		grpcolm=tokens[i+1]
		#print(len(grpcolm))
		if("DISTINCT" not in str(stmt).upper()):
			temp=tokens[1].split(",")
			tablename=tokens[3].split(",")
		else:			
			temp=tokens[2].split(",")
			tablename=tokens[4].split(",")
		flg=0
		agglist=[]
		#print(temp)
		for i in range(len(temp)):
			if("(" not in temp[i] and flg==0):
				grpsel=temp[i]
				flg=1
				if(grpsel!=grpcolm):
					#print("hi")
					print("Invalid query")
					exit()
				agglist.append(grpsel)	
			elif("(" not in temp[i] and flg==1):
				#print("bye")
				print("Invalid query")
				exit()
			else:
				agglist.append(temp[i])
		#if(flg==0):
		#	print("Invalid query")
		#	exit()
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		flg=0
		for i in tablename:
			if(grpcolm in tables[i]):
				flg=1				
				break
		if(flg==0):
			print("{0} column not present".format(grpcolm))
			exit()
		#print(aggcolm)		
		'''		
		for j in aggcolm:
			flg=0
			for i in tablename:
				if(j in tables[i]):
					flg=1
					break
			if(flg==0):
				print("{0} column not present".format(j.lower()))
				exit()
		'''		
		productcol=[]
		seq=[]
		for i in tablename:
			productcol.extend(tables[i])		
		#print(cp)		
		grpdict={}
		ind=productcol.index(grpcolm)
		if("WHERE" in str(stmt).upper()):
			ans=where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,[],0,1)
		else:
			ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		if("ORDER BY" in str(stmt).upper()):
			i=0		
			while(1):
				if(tokens[i].upper()=="GROUP BY"):
					break
				i+=1
			if("DESC" in str(stmt).upper()):
				ans.sort(key=lambda x:x[ind],reverse=True)
			else:
				ans.sort(key=lambda x:x[ind])
		for row in ans:
			if(row[ind] not in grpdict):
				grpdict[row[ind]]=[row]
				seq.append(row[ind])
			else:			
				grpdict[row[ind]].append(row)
			#print(grpdict)
		ansdict=[]
		#print(agglist)
		if("DISTINCT" in str(stmt).upper()):	
			for i in seq:
				temp=[]
				for j in agglist:
					if("(" not in j):
						temp.append(i)
						continue
					colm=j[j.index("(")+1:j.index(")")]
					#print(colm)
					if(colm not in productcol and colm!="*"):
						print("{0} column not present".format(colm.lower()))
					if(colm!="*"):
						ind=productcol.index(colm)
					if("SUM" in j.upper()):
						sum1=0
						for k in range(len(grpdict[i])):
							sum1+=grpdict[i][k][ind]
						temp.append(sum1)
					elif("MAX" in j.upper()):
						max1=-10000000000
						for k in range(len(grpdict[i])):
							max1=max(max1,grpdict[i][k][ind])
						temp.append(max1)
					elif("MIN" in j.upper()):
						min1=10000000000
						for k in range(len(grpdict[i])):
							min1=min(min1,grpdict[i][k][ind])
						temp.append(min1)
					elif("COUNT" in j.upper()):
						temp.append(len(grpdict[i]))
					elif("AVG" in j.upper()):
						sum1=0.0
						for k in range(len(grpdict[i])):
							sum1+=grpdict[i][k][ind]
						temp.append(float(sum1/len(grpdict[i])))
				if(temp not in ansdict):
					ansdict.append(temp[:])
		else:
			ansdict=[]
			#print(agglist)
			#print(grpdict)		
			for i in seq:
				temp=[]
				for j in agglist:
					if("(" not in j):
						temp.append(i)
						continue
					colm=j[j.index("(")+1:j.index(")")]
					#print(colm)
					if(colm not in productcol and colm!="*"):
						print("{0} column not present".format(colm.lower()))
					if(colm!="*"):
						ind=productcol.index(colm)
					if("SUM" in j.upper()):
						sum1=0
						for k in range(len(grpdict[i])):
							sum1+=grpdict[i][k][ind]
						temp.append(sum1)
					elif("MAX" in j.upper()):
						max1=-10000000000
						for k in range(len(grpdict[i])):
							max1=max(max1,grpdict[i][k][ind])
						temp.append(max1)
					elif("MIN" in j.upper()):
						min1=10000000000
						for k in range(len(grpdict[i])):
							min1=min(min1,grpdict[i][k][ind])
						temp.append(min1)
					elif("COUNT" in j.upper()):
						temp.append(len(grpdict[i]))
					elif("AVG" in j.upper()):
						sum1=0.0
						for k in range(len(grpdict[i])):
							sum1+=grpdict[i][k][ind]
						temp.append(float(sum1/len(grpdict[i])))
				#print(temp,i,j)
				ansdict.append(temp[:])
		for i in agglist:
			print(i.lower()),
		print
		for i in ansdict:
			for j in i:
				print(j),
			print
		exit()		
	'''
	if("WHERE" in str(stmt).upper()):
		tablename=[]
		colmname=[]
		productcol=[]
		#print(colmname,tablename)
		if("DISTINCT" in str(stmt).upper()):
			tokens[4]=tokens[4].replace(" ","")
			tablename=tokens[4].split(",")
		else:
			tokens[3]=tokens[3].replace(" ","")
			tablename=tokens[3].split(",")
		for i in tablename:
			productcol.extend(tables[i])			
		if("*" in str(stmt)):
			if("DISTINCT" in str(stmt).upper()):			
				tokens[2]=""
				for i in range(len(productcol)-1):
					tokens[2]+=productcol[i]+","
				tokens[2]+=productcol[-1]

			else:
				tokens[1]=""
				for i in range(len(productcol)-1):
					tokens[1]+=productcol[i]+","
				tokens[1]+=productcol[-1]
		ans=where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,colmname)
	'''
	#------------------------------check for aggregate functions------------------------------

	if any(ext in tokens[1].upper() for ext in agg):
		agglist=tokens[1].split(",")
		colm=[]
		for i in agglist:
			if("(" not in i):
				print("Invalid query")
				exit()
			if("*" in i):
				colm.append("*")
			else:
				colm.append(i[i.index("(")+1:i.index(")")])
		tablename=tokens[3].split(",")
		if(tokens[2].upper()!="FROM"):
			print("Invalid query")
		flg=0
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		for j in colm:		
			if(j!="*"):
				for i in tablename:
					if(j in tables[i]):
						flg=1				
						break
				if(flg==0):
					print("{0} column not present".format(j.lower()))
					exit()			
		if("WHERE" in str(stmt).upper()):
			ans=where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,colm,1,0)
		else:
			ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		productcol=[]
		for i in tablename:
			productcol.extend(tables[i])
		#print(ans,colm,productcol)	
		fin=[]
		for i in range(len(agglist)):	
			if("SUM" in agglist[i].upper()):
				sum1=0
				ind=productcol.index(colm[i])
				for row in ans:
					sum1+=row[ind]
				fin.append(sum1)
			elif("COUNT" in agglist[i].upper()):
				fin.append(len(ans))
			elif("MIN" in agglist[i].upper()):
				min1=1000000000
				ind=productcol.index(colm[i])
				for row in ans:
					min1=min(min1,row[ind])
				fin.append(min1)
			elif("MAX" in agglist[i].upper()):
				max1=-1000000000
				ind=productcol.index(colm[i])
				for row in ans:
					max1=max(max1,row[ind])
				fin.append(max1)
			elif("AVG" in agglist[i].upper()):
				sum1=0.0
				ind=productcol.index(colm[i])
				for row in ans:
					sum1+=row[ind]
				fin.append(float(sum1/len(ans)))
		for i in fin:
			print(i),
		exit()
	if any(ext in tokens[2].upper() for ext in agg) and tokens[1].upper()=="DISTINCT":
		agglist=tokens[2].split(",")
		colm=[]
		for i in agglist:
			if("(" not in i):
				print("Invalid query")
				exit()
			if("*" in i):
				colm.append("*")
			else:
				colm.append(i[i.index("(")+1:i.index(")")])
		#print(colm)
		tablename=tokens[4].split(",")
		if(tokens[3].upper()!="FROM"):
			print("Invalid query")
		flg=0
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		for j in colm:		
			if(j!="*"):
				for i in tablename:
					if(j in tables[i]):
						flg=1				
						break
				if(flg==0):
					print("{0} column not present".format(j.lower()))
					exit()			
		if("WHERE" in str(stmt).upper()):
			ans=where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,colm,1,0)
		else:
			ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		productcol=[]
		for i in tablename:
			productcol.extend(tables[i])
		fin=[]		
		for i in range(len(agglist)):
			if(colm[i]=="*"):
				list1=[]
				count1=0
				for row in ans:
					list1.append(row)					
					count1+=1
				fin.append(count1)
			#print(ans,colm,productcol)		
			elif("SUM" in agglist[i].upper()):
				sum1=0
				ind=productcol.index(colm[i])
				list1=[]
				for row in ans:
					list1.append(row[ind])					
					sum1+=row[ind]
				fin.append(sum1)
			elif("COUNT" in agglist[i].upper()):
				count1=0
				list1=[]
				ind=productcol.index(colm[i])
				for row in ans:
					list1.append(row[ind])				
					count1+=1			
				fin.append(count1)
			elif("MIN" in agglist[i].upper()):
				min1=1000000000
				list1=[]
				ind=productcol.index(colm[i])
				for row in ans:
					list1.append(row[ind])
					min1=min(min1,row[ind])
				fin.append(min1)
			elif("MAX" in agglist[i].upper()):
				max1=-1000000000
				ind=productcol.index(colm[i])
				list1=[]
				for row in ans:
					list1.append(row[ind])
					max1=max(max1,row[ind])
				fin.append(max1)
			elif("AVG" in agglist[i].upper()):
				list1=[]				
				sum1=0.0
				count1=0
				ind=productcol.index(colm[i])
				for row in ans:
					list1.append(row[ind])				
					sum1+=row[ind]
					count1+=1
				fin.append(float(sum1/count1))
		for i in fin:
			print(i),		
		exit()
	'''
	elif(tokens[1].upper()=="DISTINCT"):
		colmnames=tokens[2].split(",")
		#print(colmnames)
		if(
	'''	
#---------------------------------------where and other queries-------------------------------------------------------
	if("WHERE" in str(stmt).upper()):
		tablename=[]
		colmname=[]
		productcol=[]
		#print(colmname,tablename)
		if("DISTINCT" in str(stmt).upper()):
			tokens[4]=tokens[4].replace(" ","")
			tablename=tokens[4].split(",")
		else:
			tokens[3]=tokens[3].replace(" ","")
			tablename=tokens[3].split(",")
		for i in tablename:
			productcol.extend(tables[i])			
		if("*" in str(stmt)):
			if("DISTINCT" in str(stmt).upper()):			
				tokens[2]=""
				for i in range(len(productcol)-1):
					tokens[2]+=productcol[i]+","
				tokens[2]+=productcol[-1]

			else:
				tokens[1]=""
				for i in range(len(productcol)-1):
					tokens[1]+=productcol[i]+","
				tokens[1]+=productcol[-1]
		ans=where(stmt,tables,tablecontent,tokens,tablecontentrow,tablename,colmname,0,0)
		if("DISTINCT" in str(stmt).upper()):
			colmname=tokens[2].split(",")
		else:		
			colmname=tokens[1].split(",")
	elif(len(tokens)==5 or len(tokens)==8 or len(tokens)==7 or len(tokens)==6):
		if("DISTINCT" in str(stmt).upper()):
			tokens[2]=tokens[2].replace(" ","")
			colmname=tokens[2].split(",")
			tokens[4]=tokens[4].replace(" ","")
			tablename=tokens[4].split(",")
			wheretoken=tokens[5].upper()
		else:
			tokens[1]=tokens[1].replace(" ","")
			colmname=tokens[1].split(",")
			tokens[3]=tokens[3].replace(" ","")
			tablename=tokens[3].split(",")
			wheretoken=tokens[4].upper()		
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		productcol=[]		
		for i in tablename:
			productcol.extend(tables[i])		
		for j in colmname:
			flg=0
			if(j=="*"):
				continue
			for i in tablename:
				if(j in tables[i]):
					flg=1
					break
			if(flg==0):
				#print("Hi")
				print("{0} column not present".format(j.lower()))
				exit()	
		ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		
		indices=[]
		if(colmname==["*"]):
			colmname=productcol[:]
		#print(colmname)
		for j in colmname:
			indices.append(productcol.index(j))		
		'''		
		fin=[]		
		for row in ans:
			temp=[]
			for index in indices:
				temp.append(row[index])
			fin.append(temp)
		'''
	#else:
	#	print("Invalid command")
	#	exit()
	if("ORDER BY" in str(stmt).upper()):
		colmorder=tokens[-2].split()[0]
		#print(colmorder)
		#if(colmorder not in colmname):
		#	print("Invalid query")
		#	exit()
		'''
		productcol=[]		
		for i in tablename:
			productcol.extend(tables[i])		
		'''		
		ind=productcol.index(colmorder)
		#print(ind)
		#print(ans)
		if("DESC" in str(stmt).upper()):
			ans.sort(key=lambda x:x[ind],reverse=True)
		else:
			ans.sort(key=lambda x:x[ind])				 
	if("DISTINCT" not in str(stmt).upper()):
		"""
		tokens[1]=tokens[1].replace(" ","")		
		colmname=tokens[1].split(",")
		tokens[3]=tokens[3].replace(" ","")
		tablename=tokens[3].split(",")
		"""
		#print(tokens[2],tokens[4])
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		#print(colmname)		
		for j in colmname:
			flg=0
			if(j=="*"):
				continue
			for i in tablename:
				if(j in tables[i]):
					flg=1
					break
			if(flg==0):
				#print("bye")
				print("{0} column not present".format(j.lower()))
				exit()
		#ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		productcol=[]		
		for i in tablename:
			productcol.extend(tables[i])							
		indices=[]
		for j in colmname:
			indices.append(productcol.index(j))	
		fin=[]		
		for row in ans:
			temp=[]
			for index in indices:
				temp.append(row[index])
			fin.append(temp)	
	else:
		"""		
		tokens[2]=tokens[2].replace(" ","")		
		colmname=tokens[2].split(",")
		tokens[4]=tokens[4].replace(" ","")
		tablename=tokens[4].split(",")
		"""
		#print(colmname)
		#print(ans)	
		#print(tokens[2],tokens[4])
		for i in tablename:
			if(i not in tables):
				print("{0} table not present in database".format(i.lower()))
				exit()
		for j in colmname:
			flg=0
			if(j=="*"):
				continue
			for i in tablename:
				if(j in tables[i]):
					flg=1
					break
			if(flg==0):
				print("{0} column not present".format(j.lower()))
				exit()
		#ans=cartesian(tables,tablecontent,tablename,tablecontentrow)
		productcol=[]		
		for i in tablename:
			productcol.extend(tables[i])							
		indices=[]
		for j in colmname:
			indices.append(productcol.index(j))	
		fin=[]		
		for row in ans:
			temp=[]
			for index in indices:
				temp.append(row[index])
			if(temp not in fin):
				fin.append(temp)	
	for i in colmname:
		print(i.lower()),
		print(" "),
		print(" "),	
	print
	if(fin==[]):
		print
	for row in fin:
		for index in range(len(row)):
			print(row[index]),
			print(" "),
		print
def main():
	tables={}
	f=open("metadata.txt","r")
	inp="hi"	
	flg=0
	tn=""
	while(inp!=""):
		inp=f.readline().split("\n")[0]
		if(inp==""):
			break
		#print(inp)
		if(inp=="<begin_table>" or inp=="<end_table>"):
			flg=0			
			continue
		elif(flg==0):
			tables[inp]=[]
			flg=1
			tn=inp
		else:
			tables[tn].append(inp)
	#print(tables)	
	f.close()
	tablecontent={}
	list1=list(tables.keys())
	tablecontentrow={}	
	for i in list1:
		tablecontent[i]=[]
		tablecontentrow[i]=[]		
		for j in range(len(tables[i])):
			tablecontent[i].append([])
		with open(i+'.csv', 'r') as file1:
			reader = csv.reader(file1)
			cur=0
			for row in reader:
				tablecontentrow[i].append([])
				for j in range(len(row)):
					tablecontent[i][j].append(int(row[j].replace("\"","")))
					tablecontentrow[i][cur].append(int(row[j].replace("\"","")))
				cur+=1
	#print(tablecontentrow)
	queries=sys.argv[1]
	#print(queries)
	tokenization(queries,tables,tablecontent,tablecontentrow)
main()
'''		
		if("DISTINCT" in str(stmt).upper()):
			#tokens[2]=str(tokens[2])
			tokens[2]=tokens[2].replace(" ","")
			colmname=tokens[2].split(",")
			#tokens[4]=str(tokens[4])
			tokens[4]=tokens[4].replace(" ","")
			tablename=tokens[4].split(",")
			wheretoken=tokens[5].upper()
		else:
			tokens[1]=tokens[1].replace(" ","")
			colmname=tokens[1].split(",")
			tokens[3]=tokens[3].replace(" ","")
			tablename=tokens[3].split(",")
			wheretoken=tokens[4].upper()			
		#print(colmname)
		#print(ans)
		#print(tokens[2],tokens[4])		
		indices=[]
		for j in colmname:
			indices.append(productcol.index(j))		
		if(ans==[]):
			fin=[]
		else:		
			if("DISTINCT" in str(stmt).upper()):
				tokens[2]=tokens[2].replace(" ","")
				colmname=tokens[2].split(",")
				tokens[4]=tokens[4].replace(" ","")
				tablename=tokens[4].split(",")
				wheretoken=tokens[5].upper()
			else:
				tokens[1]=tokens[1].replace(" ","")
				colmname=tokens[1].split(",")
				tokens[3]=tokens[3].replace(" ","")
				tablename=tokens[3].split(",")
				wheretoken=tokens[4].upper()			
			#print(ans)		
			productcol=[]
			#print(colmname,tablename)		
			for i in tablename:
				productcol.extend(tables[i])
			indices=[]
			for j in colmname:
				indices.append(productcol.index(j))
			if("DISTINCT" in str(stmt).upper()):
				fin=[]			
				for row in ans:
					temp=[]
					for index in indices:
						temp.append(row[index])
					if(temp not in fin):
						fin.append(temp)
				#print(fin)
			else:
				fin=[]
				for row in ans:
					temp=[]
					for index in indices:
						temp.append(row[index])
					fin.append(temp)
	'''
