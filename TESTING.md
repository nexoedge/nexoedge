# Testing Guide

## Component Tests

These test programs can be run independently on one machine.

- `coding_test`: Verify the correctness of all coding schemes and report the performance of coding operations
  - Usage: `$ ./coding_test <seed_for_randomness> <file> [file ...]`
- `agent_test`: Verify the correctness of chunk requests handling at Agent, and print the network usage
  - Usage: `$ ./agent_test`
- `container_test`: Verify the correctness of container operations
  - Usage: `$ ./container_test`
- `coordinator_test`: Verify the correctness of Agent coordinator and Proxy operations
  - Usage: `$ ./coordinator_test`

### Build

Build all the test programs for component tests in the `bin` folder: `agent_test`, `coding_test`, `container_test`, `coordinator_test`

Build all test programs,

```bash
make tests
```

Optionally, to build any one of the test programs, e.g. `coding_test`,

```bash
make coding_test
```

### Testing

1. Copy all sample configuration file in the directory `sample/` to the working directory, e.g., the `build` folder.
   
   ```bash
   cp <ncloud source root directory>/sample/*.ini
   ```

2. Generate a test file
   
   ```bash
   dd if=/dev/urandom of=./1MB bs=1M count=1
   ```

3. Run the container test, which tests all chunk operations of a specific type of container 
   (see [the configuration guide](CONFIG.md) for choosing a container type) 
   
   ```bash
   ./bin/container_test
   ```

4. Run the Agent test, which tests the handling all types of chunk requests
   
   ```bash
   ./bin/agent_test
   ```

5. Run the coding test, which tests all coding operations on the specified file. The first argument is a random seed number, and the second one is the file name.
   
   ```bash
   ./bin/coding_test 123 1MB
   ```

6. Run the coordinator test, which tests the Proxy coordinator on handling newly joined Agents
   
   ```bash
   ./bin/coordinator_test
   ```
