public class PiCalc {
	private long num_steps = 5000000;

	public void run() {

		int i = 0;
		double x = 0.0, pi = 0.0, sum = 0.0;

		double step = 1.0 / (double) num_steps;

		for (i = 0; i < num_steps; i++) {
			x = (i + 0.5) * step;
			sum = sum + 4.0 / (1.0 + x * x);
		}

		pi = step * sum;

		System.out.println("pi = " + pi);
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		PiCalc pi = new PiCalc();
		pi.run();
	}

}
