
import argparse
import logging

from solver import meshSolver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Mesh Solver Ongoing Pipeline")
    parser.add_argument("--environment_name", default="dap_prod", help="Database name (e.g., dap_prod)")
    args, _ = parser.parse_known_args()

    logger.info(f"Environment: {args.environment_name}")

    # Initialize and run Source Solver
 
     mysolver=meshSolver('default')
     mysolver.sourcedSolverData()


    

    logger.info("Source mesh solver update process completed successfully")


if __name__ == "__main__":
    main()