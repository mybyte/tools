package de.miba.neo4j.rmiservice;

class PerformanceTracker {
	long startTime;
	long lastTime;

	long processedItems;

	double speed = -1;
	
	public PerformanceTracker(){
		start();
	}

	public void start() {
		this.startTime = System.currentTimeMillis();
		this.lastTime = startTime;
		processedItems = 0;
		speed = -1;
	}

	public void tick() {
		long timePassed = System.currentTimeMillis() - lastTime;

		double currentSpeed = 1 / ((double) timePassed / 1000);

		if (speed == -1)
			speed = currentSpeed;
		else
			speed = speed * 0.9 + currentSpeed * 0.1;

		lastTime = System.currentTimeMillis();
		processedItems++;
	}

	public double getSpeed() {
		return speed;
	}

	public long getItemsProcessed() {
		return processedItems;
	}
}
