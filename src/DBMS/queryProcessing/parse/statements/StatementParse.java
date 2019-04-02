package DBMS.queryProcessing.parse.statements;

import java.sql.SQLException;

import DBMS.fileManager.Schema;
import DBMS.queryProcessing.queryEngine.Plan;
import net.sf.jsqlparser.statement.Statement;

public interface StatementParse {
	Plan parse(Statement statement,Schema schema) throws SQLException;
}
