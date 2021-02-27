# Databricks notebook source
def test_cycle_no_cycle(self):
        # test no cycle
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})
                            
        #       ------ Task2   -------- Task4          
        #Task1                  ------- Task5
        #       ----- Task3    -------- Task6
        with dag:
            op1 = DummyOperator(task_id='Task1')
            op2 = DummyOperator(task_id='Task2')
            op3 = DummyOperator(task_id='Task3')
            op4 = DummyOperator(task_id='Task4')
            op5 = DummyOperator(task_id='Task5')
            op6 = DummyOperator(task_id='Task6')
            
            op1.set_downstream(op2)
            op1.set_downstream(op3)
            op2.set_downstream(op4)
            op2.set_downstream(op5)
            op2.set_downstream(op6)
            op3.set_downstream(op4)
            op3.set_downstream(op5)
            op3.set_downstream(op6)
            
            
        self.assertFalse(test_cycle(dag)) 
