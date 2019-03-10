package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;

import DBMS.fileManager.ISchema;
import DBMS.queryProcessing.queryEngine.Plan;
import net.sf.jsqlparser.statement.Statement;

public interface StatementParse {
	Plan parse(Statement statement,ISchema schema) throws SQLException;
}
