package com.isesol.kafka;

import kafka.utils.immutable;


public class Mutilthread implements Runnable{

	static int i = 1000;
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		synchronized (this) {
			while(i> 0){
				i = i - 1;
				System.out.println(Thread.currentThread().getName() + " " + i);
		
			}
		}


		
		
	}
	
	
	public static void main(String[] args){
		
		Thread t1 = new Thread(new Mutilthread());
		Thread t2 = new Thread(new Mutilthread());
		Thread t3 = new Thread(new Mutilthread());
		Thread t4 = new Thread(new Mutilthread());
		t1.start();
		t2.start();
		t3.start();
		t4.start();
	}

}
