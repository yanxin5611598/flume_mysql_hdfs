package org.keedio.flume.source;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.opencsv.ResultSetHelper;
import com.opencsv.ResultSetHelperService;

public class MyCSVWriter implements Closeable, Flushable
{
  public static final int INITIAL_STRING_SIZE = 128;
  public static final char DEFAULT_ESCAPE_CHARACTER = '"';
  public static final char DEFAULT_SEPARATOR = ',';
  public static final char DEFAULT_QUOTE_CHARACTER = '"';
  public static final char NO_QUOTE_CHARACTER = '\000';
  public static final char NO_ESCAPE_CHARACTER = '\000';
  public static final String DEFAULT_LINE_END = "\n";
  private Writer rawWriter;
  private PrintWriter pw;
  private char separator;
  private char quotechar;
  private char escapechar;
  private String lineEnd;
  private ResultSetHelper resultService = new ResultSetHelperService();
  




  public MyCSVWriter(Writer writer)
  {
    this(writer, ',');
  }
  





  public MyCSVWriter(Writer writer, char separator)
  {
    this(writer, separator, '"');
  }
  





  /**
   * separator是分隔符，quotechar是引用符，escapechar是转义字符。当一段话中出现分隔符的时候，用引用符将这句话括起来，就能排除歧义。
   * */
  public MyCSVWriter(Writer writer, char separator, char quotechar)
  {
    this(writer, separator, quotechar, '"');
  }
  







  public MyCSVWriter(Writer writer, char separator, char quotechar, char escapechar)
  {
    this(writer, separator, quotechar, escapechar, "\n");
  }
  








  public MyCSVWriter(Writer writer, char separator, char quotechar, String lineEnd)
  {
    this(writer, separator, quotechar, '"', lineEnd);
  }
  









  public MyCSVWriter(Writer writer, char separator, char quotechar, char escapechar, String lineEnd)
  {
    this.rawWriter = writer;
    this.pw = new PrintWriter(writer);
    this.separator = separator;
    this.quotechar = quotechar;
    this.escapechar = escapechar;
    this.lineEnd = lineEnd;
  }
  









  public void writeAll(List<String[]> allLines, boolean applyQuotesToAll)
  {
    for (String[] line : allLines) {
      writeNext(line, applyQuotesToAll);
    }
  }
  






  public void writeAll(List<String[]> allLines)
  {
    for (String[] line : allLines) {
      writeNext(line);
    }
  }
  






  protected void writeColumnNames(ResultSet rs)
    throws SQLException
  {
    writeNext(this.resultService.getColumnNames(rs));
  }
  








  public void writeAll(ResultSet rs, boolean includeColumnNames)
    throws SQLException, IOException
  {
    writeAll(rs, includeColumnNames, false);
  }
  












  public void writeAll(ResultSet rs, boolean includeColumnNames, boolean trim)
    throws SQLException, IOException
  {
    if (includeColumnNames) {
      writeColumnNames(rs);
    }
    
    while (rs.next()) {
      writeNext(this.resultService.getColumnValues(rs, trim));
    }
  }
  








  public void writeNext(String[] nextLine, boolean applyQuotesToAll)
  {
    if (nextLine == null) {
      return;
    }
    
    StringBuilder sb = new StringBuilder(128);
    for (int i = 0; i < nextLine.length; i++)
    {
      if (i != 0) {
        sb.append(this.separator);
      }
      
      String nextElement = nextLine[i];
      
      if (nextElement != null)
      {
    	  	//遇到json文件会将其进一步解析
        Boolean stringContainsSpecialCharacters = Boolean.valueOf(stringContainsSpecialCharacters(nextElement));
        if (((applyQuotesToAll) || (stringContainsSpecialCharacters.booleanValue() && !isJSON(nextElement))) && (this.quotechar != 0)) {
          sb.append(this.quotechar);
        }
        
        if (stringContainsSpecialCharacters.booleanValue() && !isJSON(nextElement)) {
          sb.append(processLine(nextElement));
        } else {
        		//对JSON字串不处理直接append
        		sb.append(nextElement);
        }
        
        if (((applyQuotesToAll) ||  (stringContainsSpecialCharacters.booleanValue() && !isJSON(nextElement))) && (this.quotechar != 0)) {
          sb.append(this.quotechar);
        }
      }
    }
    sb.append(this.lineEnd);
    this.pw.write(sb.toString());
  }
  

  /**
   * 判断字符串是否为JSON格式
   * @return true:是JSON格式的字符串
   * 				false:不是JSON格式的字符串*/
  public boolean isJSON(String str){
	  try{
		  JSONObject.parseObject(str);
	  }catch(JSONException e){
		  return false;
	  }
	  return true;
  } 



  public void writeNext(String[] nextLine)
  {
    writeNext(nextLine, true);
  }
  




  private boolean stringContainsSpecialCharacters(String line)
  {
    return (line.indexOf(this.quotechar) != -1) || (line.indexOf(this.escapechar) != -1) || (line.indexOf(this.separator) != -1) || (line.contains("\n")) || (line.contains("\r"));
  }
  




  protected StringBuilder processLine(String nextElement)
  {
    StringBuilder sb = new StringBuilder(128);
    for (int j = 0; j < nextElement.length(); j++) {
      char nextChar = nextElement.charAt(j);
      processCharacter(sb, nextChar);
    }
    
    return sb;
  }
  




  private void processCharacter(StringBuilder sb, char nextChar)
  {
    if ((this.escapechar != 0) && ((nextChar == this.quotechar) || (nextChar == this.escapechar))) {
      sb.append(this.escapechar).append(nextChar);
    } else {
      sb.append(nextChar);
    }
  }
  




  public void flush()
    throws IOException
  {
    this.pw.flush();
  }
  




  public void close()
    throws IOException
  {
    flush();
    this.pw.close();
    this.rawWriter.close();
  }
  






  public boolean checkError()
  {
    return this.pw.checkError();
  }
  



  public void setResultService(ResultSetHelper resultService)
  {
    this.resultService = resultService;
  }
  

  public void flushQuietly()
  {
    try
    {
      flush();
    }
    catch (IOException e) {}
  }
}
