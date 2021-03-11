import logo from './logo.svg';
import styled from 'styled-components';
import React, { useState } from 'react';
import {dummyRows} from './dummyData';
import Rows from './Rows';
import Select from './Select';
import Insert from './Insert';
import Update from './Update';
import Delete from './Delete';

const SELECT = 'SELECT';
const INSERT = 'INSERT';
const UPDATE = 'UPDATE';
const DELETE = 'DELETE';

function App() {
  let [rows,setRows] = useState(dummyRows);
  let [mode,setMode] = useState(INSERT);

  let selectedControls = <Select/>;
  if(mode === INSERT){
    selectedControls = <Insert/>
  }
  if(mode === UPDATE){
    selectedControls = <Update/>
  }
  if(mode === DELETE){
    selectedControls = <Delete/>
  }


  return (
    <AppContainer>
      <h1>Milk Management System</h1>
      <h2>Powered by Sparkle Motion DB</h2>
      <Rows rows={rows}/>
      <ControlsContainer>
        <ModeButton onClick={()=>{setMode(SELECT)}} mode={mode} value={SELECT}>select</ModeButton>
        <ModeButton onClick={()=>{setMode(INSERT)}} mode={mode} value={INSERT}>insert</ModeButton>
        <ModeButton onClick={()=>{setMode(UPDATE)}} mode={mode} value={UPDATE}>update</ModeButton>
        <ModeButton onClick={()=>{setMode(DELETE)}} mode={mode} value={DELETE}>delete</ModeButton>
        {selectedControls}
      </ControlsContainer>
    </AppContainer>
  );
}

const ModeButton = styled.button`
background-color:${props=>props.mode===props.value?'black':'white'};
color:${props=>props.mode===props.value?'white':'black'};
margin:30px 10px 10px 0px;
border:1px solid;
border-color:${props=>props.mode===props.value?'black':'#dbdbdb'};
border-radius:4px;
padding:5px;
font-size:15px;

transition:border-color 0.2s, color 0.2s, background-color 0.2s;

:hover{
  background-color:#dbdbdb;
  color:black;
  border-color:#dbdbdb;
}
`;

const AppContainer = styled.div`
  margin:auto;
  max-width:500px;
  padding:5vw;
  padding-top:15px;

`;

const ControlsContainer = styled.div`

`;

export default App;
