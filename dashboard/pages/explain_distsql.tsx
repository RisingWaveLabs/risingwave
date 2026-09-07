import { Box, Button, Stack, Textarea } from "@chakra-ui/react"
import { Fragment, SetStateAction, useState } from "react"
import styled from "styled-components"
import GraphvizComponent from "../components/GraphvizComponent"
import Title from "../components/Title"

const ContainerDiv = styled(Box)`
  font-family: sans-serif;
  text-align: left;
`

const DemoArea = styled(Box)`
  width: 100%;
  height: 80vh;
`

export default function Explain() {
  const [input, setInput] = useState("")
  const [isUpdate, setIsUpdate] = useState(false)
  const [isDotParsed, setIsDotParsed] = useState(false)

  const handleChange = (event: {
    target: { value: SetStateAction<string> }
  }) => {
    setInput(event.target.value)
    setIsUpdate(true)
  }

  const handleClick = () => {
    if (!isUpdate) return
    setIsDotParsed(true)
  }

  return (
    <Fragment>
      <Box p={3}>
        <Title>Render Graphviz Dot format</Title>
        <Stack direction="row" spacing={4} align="center">
          <Textarea
            name="input graph"
            placeholder="Input DOT"
            value={input}
            onChange={handleChange}
            style={{ width: "1000px", height: "100px" }}
          />
          <Button
            colorScheme="green"
            onClick={handleClick}
            style={{ width: "80px", height: "100px" }}
          >
            Parse
          </Button>
        </Stack>

        <ContainerDiv>
          <DemoArea>
            {/* Render Graphviz visualization only when DOT input is provided */}
            {isDotParsed && input && (
              <Box mt={4}>
                <GraphvizComponent dot={input} />
              </Box>
            )}
          </DemoArea>
        </ContainerDiv>
      </Box>
    </Fragment>
  )
}
