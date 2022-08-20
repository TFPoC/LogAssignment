package hackerrank;


import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import static junit.framework.TestCase.assertEquals;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class LogAssignmentTest {

    private static LogAssignment logAssignment;

    String test = "{\"id\":\"scsmbstgra\", \"state\":\"STARTED\", \"type\":\"APPLICATION_LOG\", \"host\":\"12345\", \"timestamp\":1491377495212}";

    @BeforeClass
    public static void instantiate() {
        logAssignment = new LogAssignment();
    }

    @Test
    public void toObject() {

        Event e = LogAssignment.toObject(test);

        assertEquals("scsmbstgra", e.getId());
    }
}
