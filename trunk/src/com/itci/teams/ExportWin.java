package com.itci.teams;

import com.itci.teams.ui.MainForm;

import javax.swing.*;


public class ExportWin {
    public static void main(String[] args) {
        final MainForm form = new MainForm();
        final JFrame jFrame = new JFrame("SQL saver");
        jFrame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        jFrame.setContentPane( form.getMainPanel() );
        jFrame.setSize(800,600);
        jFrame.setVisible(true);
    }
}
