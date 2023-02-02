package com.deepexi;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple TarimServer.
 */
public class TarimServerTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public TarimServerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( TarimServerTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testTarimServer()
    {
        assertTrue( true );
    }
}
