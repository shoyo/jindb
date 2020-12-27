/*
 * Copyright (c) 2020.  Shoyo Inokuchi.
 * Please refer to github.com/shoyo/jin for more information about this project and its license.
 */

use jin::execution::plans::QueryPlanNode;

/// Tests for query execution.
/// A query plan is a tree structure constructed out of plan nodes. During execution, the query
/// plan tree is traversed and an executor is constructed at every plan node.
/// Executors are responsible for producing records for the caller according to the "Volcano Model".

/// Return the following query plan:
///
///
///
/// This plan reflects the SQL statement: "SELECT id, name from STUDENTS;"
fn setup() -> Box<dyn QueryPlanNode> {
    todo!()
}

#[test]
fn test_execute_query_plan() {}
