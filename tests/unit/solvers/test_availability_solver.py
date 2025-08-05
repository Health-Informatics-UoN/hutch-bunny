import pytest
from unittest.mock import Mock, MagicMock
from sqlalchemy import func, text
from sqlalchemy.sql.elements import ClauseElement
from sqlalchemy.dialects import postgresql, mssql, mysql, sqlite

from hutch_bunny.core.solvers.availability_solver_refactor import AvailabilitySolver, SQLDialectHandler


class TestSQLDialectHandler:

    def test_get_year_difference_unsupported_dialect(self):
        """Test unsupported dialect raises NotImplementedError."""
        engine = Mock()
        engine.dialect.name = "mysql"

        start_date = Mock(spec=ClauseElement)
        birth_date = Mock(spec=ClauseElement)

        with pytest.raises(NotImplementedError, match="Unsupported database dialect"):
            SQLDialectHandler.get_year_difference(engine, start_date, birth_date)

    def test_get_year_difference_postgresql(self):
        """Test PostgreSQL dialect returns correct function."""
        # Arrange
        engine = Mock()
        engine.dialect.name = "postgresql"

        start_date = Mock(spec=ClauseElement)
        birth_date = Mock(spec=ClauseElement)

        # Act
        result = SQLDialectHandler.get_year_difference(engine, start_date, birth_date)

        # Assert
        # Check the result is a SQLAlchemy expression
        assert str(result) == str(
            func.date_part("year", start_date) - func.date_part("year", birth_date)
        )

    def test_get_year_difference_mssql(self):
        """Test MSSQL dialect returns correct function."""
        # Arrange
        engine = Mock()
        engine.dialect.name = "mssql"

        start_date = Mock(spec=ClauseElement)
        birth_date = Mock(spec=ClauseElement)

        # Act
        result = SQLDialectHandler.get_year_difference(engine, start_date, birth_date)

        # Assert
        assert str(result) == str(
            func.DATEPART(text("year"), start_date) - func.DATEPART(text("year"), birth_date)
        )

    def test_get_year_difference_with_actual_column_elements(self):
        """Test with actual SQLAlchemy column elements."""
        from sqlalchemy import Column, Date, create_engine
        from sqlalchemy.ext.declarative import declarative_base

        Base = declarative_base()

        class TestTable(Base):
            __tablename__ = 'test'
            start_date = Column(Date)
            birth_date = Column(Date)

        # Test with PostgreSQL
        pg_engine = create_engine("postgresql://user:pass@localhost/db")
        result = SQLDialectHandler.get_year_difference(
            pg_engine,
            TestTable.start_date,
            TestTable.birth_date
        )

        # Verify it produces valid SQL when compiled
        compiled = str(result.compile(dialect=postgresql.dialect()))
        assert "date_part" in compiled
        assert "year" in compiled


class TestOMOPRuleQueryBuilder:

    def test_add_concept_constraint(self):
        pass

    def add_age_constraint(self):
        pass


