
import java.util.*;
import java.io.*;
import java.net.*;
import java.lang.*;
import java.util.concurrent.*;



@SuppressWarnings("unchecked")
 public class ExoRedis {

    public static void main(String[] args) {
        try {

      String fileName = "";
      final  HashMap<String,String> getset = new HashMap<String,String>();
      final  HashMap<String,SortedMap> sets = new HashMap<String,SortedMap>();
      SortedMap subMapT= new TreeMap();
      List<String> rangePrinter = new ArrayList<>();;

      if (args.length == 0)
		    {
                        System.out.println ("No File specified! Default file is exoredis.rdb");
                        fileName= "exoredis.rdb";
		    }
        else {fileName=args[0];}

        File varTmpDir = new File(fileName);
		boolean exists = varTmpDir.exists();
    int i=0;
    int j=0;
    int count=0;
    Iterator itti;
    Set set;
    byte[] bytes;
    String s;
    StringBuffer binary = new StringBuffer();
    char oldValue;
		if(exists){
        FileInputStream fis = new FileInputStream("exoredis.rdb");
        ObjectInputStream ois = new ObjectInputStream(fis);

        HashMap<String,String>  getsettemp =   (HashMap<String,String>)ois.readObject();
        HashMap<String,SortedMap>  setstemp = (HashMap<String,SortedMap>)ois.readObject();

        getset.putAll(getsettemp);
        sets.putAll(setstemp);

        ois.close();
    	}
    	else
    	{


    		FileOutputStream fos = new FileOutputStream("exoredis.rdb");
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(getset);
        oos.writeObject(sets);
        oos.close();


    	}
       class saver extends Thread{

        public void run() throws RuntimeException{

            try{


              FileOutputStream fos = new FileOutputStream("exoredis.rdb");
                   ObjectOutputStream oos = new ObjectOutputStream(fos);
                   oos.writeObject(getset);
                   oos.writeObject(sets);
                   oos.close();
                   System.out.println("SAVING...");

                 }

                 catch(Exception e){

                 }
        }}




      ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
      //intercept SIGINT, SIGKILL and  other shutdown Signals
      Runtime.getRuntime().addShutdownHook(new saver());
    	String fullCommand="";

      ServerSocket serverSocket = new ServerSocket(15000);
			Socket socket = serverSocket.accept();
      OutputStream ostream = socket.getOutputStream();
      PrintWriter pwrite = new PrintWriter(ostream, true);
      BufferedReader brin = new BufferedReader (new InputStreamReader (socket.getInputStream ()));



				while(true)
        {
          try
          {
          if((fullCommand = brin.readLine()) != null)

          {

          // System.out.println(fullCommand);


					String[] commands=fullCommand.split("\\s+|\\t+");
					switch(commands[0].toUpperCase())
					{
						case "SAVE":
								FileOutputStream fos = new FileOutputStream("exoredis.rdb");
       							 ObjectOutputStream oos = new ObjectOutputStream(fos);
       							 oos.writeObject(getset);
       							 oos.writeObject(sets);
       							 oos.close();
       							 pwrite.flush();
                     pwrite.println("+OK\r\n");
								break;
						case "GET":
                try{
                pwrite.flush();
                pwrite.println(RedisProto.Encode((String)getset.get(commands[1])));
                }
                catch(NullPointerException e){
                    pwrite.flush();
                    pwrite.println("$-1\r\n");

                }

								break;
						case "SET":
            if (commands.length== 3){
            getset.put(commands[1], commands[2]);
            pwrite.flush();
            pwrite.println(RedisProto.Encode("+OK"));}

            else if(commands.length<3){

              pwrite.flush();
              pwrite.println("-ERR wrong number of arguments for 'SET' command\r\n");


            }
            else
            {
              i=3;
              while(i!=commands.length){

              if(commands[i].toUpperCase().equals("EX")){

                if(commands.length>(i+2)){
                  if (commands[i+2].toUpperCase().equals("XX")){

                    if(getset.containsKey(commands[1])){
                      getset.put(commands[1], commands[2]);
                      scheduler.schedule(new Runnable() { public void run() {
                      getset.remove( commands[1]);
                      }}, Long.parseLong(commands[i+1], 10) , TimeUnit.SECONDS);
                    pwrite.flush();
                    pwrite.println(RedisProto.Encode("+OK"));
                  }
                    else{
                        pwrite.flush();
                        pwrite.println("$-1\r\n");
                    }
                    i+=3;

                  }
                  else if(commands[i+2].toUpperCase().equals("NX")){

                      if(!getset.containsKey(commands[1])){
                        getset.put(commands[1], commands[2]);
                        scheduler.schedule(new Runnable() { public void run() {
                        getset.remove( commands[1]);
                        }}, Long.parseLong(commands[i+1], 10) , TimeUnit.SECONDS);
                          pwrite.flush();
                          pwrite.println(RedisProto.Encode("+OK"));
                    }
                      else{
                          pwrite.flush();
                          pwrite.println("$-1\r\n");
                      }
                    i+=3;

                  }

                }
                else{

                getset.put(commands[1], commands[2]);
                scheduler.schedule(new Runnable() { public void run() {
                  getset.remove( commands[1]);
                }}, Long.parseLong(commands[i+1], 10) , TimeUnit.SECONDS);
                i+=2;
                pwrite.flush();
                pwrite.println(RedisProto.Encode("+OK"));
                }
                }

              else if(commands[i].toUpperCase().equals("PX")){

                if(commands.length>(i+2)){
                  if (commands[i+2].toUpperCase().equals("XX")){

                    if(getset.containsKey(commands[1])){
                      getset.put(commands[1], commands[2]);
                      scheduler.schedule(new Runnable() { public void run() {
                      getset.remove( commands[1]);
                      }}, Long.parseLong(commands[i+1], 10) , TimeUnit.MILLISECONDS);
                    pwrite.flush();
                    pwrite.println(RedisProto.Encode("+OK"));
                  }
                    else{
                        pwrite.flush();
                        pwrite.println("$-1\r\n");
                    }
                    i+=3;

                  }
                  else if(commands[i+2].toUpperCase().equals("NX")){

                      if(!getset.containsKey(commands[1])){
                        getset.put(commands[1], commands[2]);
                        scheduler.schedule(new Runnable() { public void run() {
                        getset.remove( commands[1]);
                        }}, Long.parseLong(commands[i+1], 10) , TimeUnit.MILLISECONDS);
                          pwrite.flush();
                          pwrite.println(RedisProto.Encode("+OK"));
                    }
                      else{
                          pwrite.flush();
                          pwrite.println("$-1\r\n");
                      }
                    i+=3;

                  }

                }
                else{


                getset.put(commands[1], commands[2]);
                scheduler.schedule(new Runnable() { public void run() {
                  getset.remove( commands[1]);
                }}, Long.parseLong(commands[i+1], 10) , TimeUnit.MILLISECONDS);
                i+=2;
                pwrite.flush();
                pwrite.println(RedisProto.Encode("+OK"));
              }
              }

              else if(commands[i].toUpperCase().equals("XX")){
                if(getset.containsKey(commands[1])){
                getset.put(commands[1], commands[2]);
                pwrite.flush();
                pwrite.println(RedisProto.Encode("+OK"));
              }
                else{
                    pwrite.flush();
                    pwrite.println("$-1\r\n");
                }i+=1;}

                else if(commands[i].toUpperCase().equals("NX")){
                  if(!getset.containsKey(commands[1])){
                  getset.put(commands[1], commands[2]);
                pwrite.flush();
                pwrite.println(RedisProto.Encode("+OK"));
              }
                  else{
                      pwrite.flush();
                      pwrite.println("$-1\r\n");
                  }
                i+=1;}
                else{

                  pwrite.flush();
                  pwrite.println("-ERR syntax error\r\n");
                  i=commands.length;
                }


              }

              }

								break;
						case "GETBIT":
                  if(commands.length!=3){

                    pwrite.flush();
                    pwrite.println("-ERR wrong number of arguments for 'GETBIT' command\r\n");
                    break;

                  }
                  if(!getset.containsKey(commands[1])){
                    getset.put(commands[1], "\0");
                  }
                  s= getset.get(commands[1]);

                  binary.setLength(0);
                  binary.trimToSize();

                      for ( i = 0; i < s.length(); i++) {
                          j = s.charAt(i);
                          binary.append(String.format("%8s", Integer.toBinaryString(j)).replace(' ', '0'));
                        }


                      if (binary.capacity()<=(Integer.parseInt(commands[2])) || binary.length()<=(Integer.parseInt(commands[2]))){

                          pwrite.flush();
                          pwrite.println(":0size\r\n");
                          break;

                      }

                      oldValue=binary.charAt(Integer.parseInt(commands[2]));
                      if(oldValue=='\0'){pwrite.flush();
                              pwrite.println(":0\r\n");}
                      else {pwrite.flush();
                          pwrite.println(":"+oldValue+"\r\n");}



								break;
						case "SETBIT":
                  if(commands.length!=4){

                  pwrite.flush();
                  pwrite.println("-ERR wrong number of arguments for 'SETBIT' command\r\n");
                    break;

                  }
                if(!getset.containsKey(commands[1])){
                  getset.put(commands[1], "\0");
                }

                     s= getset.get(commands[1]);//.replace("\0", "0"); this should not be there
                  //   bytes = s.getBytes();
                     binary.setLength(0);
                     binary.trimToSize();


                        for ( i = 0; i < s.length(); i++) {
                            j = s.charAt(i);
                            binary.append(String.format("%8s", Integer.toBinaryString(j)).replace(' ', '0'));
                          }






                        if (binary.capacity()<=(Integer.parseInt(commands[2])) || binary.length()<=(Integer.parseInt(commands[2]))){

                          binary.ensureCapacity(Integer.parseInt(commands[2]));
                          binary.setLength(Integer.parseInt(commands[2])+1);
                        }
                        oldValue=binary.charAt(Integer.parseInt(commands[2]));
                        binary.setCharAt(Integer.parseInt(commands[2]), commands[3].charAt(0));

                        while(binary.length()%8!=0){binary.append(0);}

                        //back to string
                            String s3= binary.toString().replace("\0", "0");

                            String s2 = "";
                              char nextChar;

                              for(i = 0; i <= s3.length()-8; i += 8)    //going through 8 bits at a time
                              {
                                nextChar = (char)Integer.parseInt(s3.substring(i, i+8), 2);
                                s2 += nextChar;
                              }
                              getset.put(commands[1],s2);
                              if(oldValue=='\0'){pwrite.flush();
                                pwrite.println(":0\r\n");}
                              else {
                                pwrite.flush();
                                pwrite.println(":"+oldValue+"\r\n");
                              }

                	break;


						case "ZADD":

                if (commands.length!=4){

                  pwrite.flush();
                  pwrite.println("-ERR wrong number of arguments for 'ZADD' command\r\n");
                    break;

                }

                if(!sets.containsKey(commands[1])){
                sets.put(commands[1],new TreeMap()); }
                //Assuming Single inserts as mentioned in Exotel Challenge webpage
                if(sets.containsKey(commands[1]) && sets.get(commands[1]).containsKey(Long.parseLong(commands[2])) && sets.get(commands[1]).get((Long.parseLong(commands[2]))).equals(commands[3]))
                {
                   pwrite.flush();
                   pwrite.println(":0\r\n");
                }

                else
                { sets.get(commands[1]).put(Long.parseLong(commands[2],10) , commands[3].replace("\"", ""));
                pwrite.flush();
                pwrite.println(":1\r\n");

                }

								break;
						case "ZCARD":
                if (commands.length!=2){

                  pwrite.flush();
                  pwrite.println("-ERR wrong number of arguments for 'ZCARD' command\r\n");
                  break;

                }
                if(sets.containsKey(commands[1])){
                  pwrite.flush();
                  pwrite.println(":" + sets.get(commands[1]).size() + "\r\n");
                 }
                 else{

                   pwrite.flush();
                   pwrite.println(":0\r\n");
                 }


								break;
						case "ZCOUNT":

                if (commands.length!=4){

                  pwrite.flush();
                  pwrite.println("-ERR wrong number of arguments for 'ZCOUNT' command\r\n");
                  break;

                  }
                else
                {
                      if(sets.containsKey(commands[1])){
                      subMapT = sets.get(commands[1]).subMap(
                      Long.parseLong(commands[2].replaceAll("inf", "9223372036854775800").replace("+", ""),10),
                      Long.parseLong(commands[3].replaceAll("inf", "9223372036854775800").replace("+", ""),10)+1);

                      pwrite.flush();
                      pwrite.println(":"+ subMapT.size()+"\r\n");


                      }

                      else{

                        pwrite.flush();
                        pwrite.println(":0\r\n");

                      }


                }


								break;
						case "ZRANGE":
                      if (commands.length==4){
                        if(sets.containsKey(commands[1])){
                        subMapT = sets.get(commands[1]).subMap(
                        Long.parseLong(commands[2].replaceAll("inf", "9223372036854775800").replace("+", ""),10),
                        Long.parseLong(commands[3].replaceAll("inf", "9223372036854775800").replace("+", ""),10)+1);

                        if(subMapT.size()==0){
                          pwrite.flush();
                          pwrite.println("$-1\r\n");
                        }
                        else{

                          pwrite.flush();
                          pwrite.println(RedisProto.Encode((String[] )subMapT.values().toArray(new String[subMapT.size()])));


                        }


                        }

                        else{

                          pwrite.flush();
                          pwrite.println("$-1\r\n");

                        }
                      }
                      else if (commands.length==5 && commands[4].toUpperCase().equals("WITHSCORES")){
                        if(sets.containsKey(commands[1])){
                        subMapT = sets.get(commands[1]).subMap(
                        Long.parseLong(commands[2].replaceAll("inf", "9223372036854775800").replace("+", ""),10),
                        Long.parseLong(commands[3].replaceAll("inf", "9223372036854775800").replace("+", ""),10)+1);

                        if(subMapT.size()==0){
                          pwrite.flush();
                          pwrite.println("$-1\r\n");
                        }
                        else{

                            set=subMapT.entrySet();
                            itti=set.iterator();

                            while(itti.hasNext()) {
                                  Map.Entry me = (Map.Entry)itti.next();
                                  rangePrinter.add((String)me.getValue());
                                  rangePrinter.add((String)me.getKey().toString());

                            }

                           pwrite.flush();
                           pwrite.println(RedisProto.Encode((String[] )rangePrinter.toArray(new String[subMapT.size()])));
                        rangePrinter.clear();


                        }


                        }

                        else{

                          pwrite.flush();
                          pwrite.println("$-1\r\n");

                        }

                      }

                      else{
                        pwrite.flush();
                        pwrite.println("-ERR wrong number of arguments for 'ZRANGE' command\r\n");
                          break;
                        }


								break;

						case "":

              pwrite.flush();
              pwrite.println("$-1\r\r");

								break;





						default:
						//-ERR unknown command 'jjbheuehe'
            pwrite.flush();
            pwrite.println("-ERR unknown command '" +commands[0]+ "'\r\n");

					}




        }
      }

      catch(Exception e){


      }

			}






        } catch (UnknownHostException e) {
          //  e.printStackTrace();
        } catch (IOException e) {
        //    e.printStackTrace();
        } catch (ClassNotFoundException e) {
        //    e.printStackTrace();
      } catch (NoSuchElementException e) {
        //    e.printStackTrace();
        }
    }
}



class RedisDecodedInfo{
  public String Content;
  public Integer Offset;
  public RedisDecodedInfo(String Content, Integer Offset){
    this.Content = Content;
    this.Offset = Offset;
  }
}



 class RedisProto {
  public static String Encode(String[] Request){
    String[] ToReturn = new String[Request.length + 1];
    ToReturn[0] = "*" + Request.length;
    for(Integer I = 1; I <= Request.length; ++I){
      ToReturn[I] = RedisProto.Encode(Request[I - 1]);
    }
    return String.join("\r\n", ToReturn);
  }
  public static String Encode(String Request){
    return "$" + Request.length() + "\r\n" + Request;
  }
  public static String[] Decode(String Request) throws Exception {
    String Type = Request.substring(0, 1);
    Integer Index = Request.indexOf("\r\n");
    Integer Count = Integer.valueOf(Request.substring(1, Index));

    if(Type.equals("*")){
      String[] ToReturn = new String[Count];
      Integer RemovedLength = Index;
      RedisDecodedInfo Info;
      for(Integer I = 0; I < Count; ++I){
        Info = RedisProto.DecodeHelper(Request.substring(RemovedLength + 2));
        ToReturn[I] = Info.Content;
        RemovedLength = Info.Offset + RemovedLength;
      }
      return ToReturn;
    } else if(Type.equals("$")) {
      return new String[]{RedisProto.DecodeHelper(Request.substring(Count + 2)).Content};
    } else {
      throw new Exception("Invalid Data");
    }
  }
  private static RedisDecodedInfo DecodeHelper(String Request) throws Exception {
    String Type = Request.substring(0, 1);
    Integer Index = Request.indexOf("\r\n") + 2;
    Integer Count = Integer.valueOf(Request.substring(1, Index - 2));
    if(Type.equals("$")){
      return new RedisDecodedInfo(Request.substring(Index, Index + Count), Index + Count + 2);
    } else if(Type.equals("-")){
      throw new Exception(Request.substring(1, Index));
    } else {
      throw new Exception("Unknown Data Type");
    }
  }
}
